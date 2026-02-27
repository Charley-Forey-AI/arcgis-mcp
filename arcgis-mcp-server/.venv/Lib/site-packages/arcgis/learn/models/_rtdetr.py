from pathlib import Path
import json
import warnings
from ._model_extension import ModelExtension

try:
    import fastai
    from fastai.vision import flatten_model, ImageList
    from fastai.vision import imagenet_stats
    import torch
    from fastai.torch_core import split_model_idx
    from .._utils.pascal_voc_rectangles import ObjectDetectionCategoryList
    from .._utils.common import get_multispectral_data_params_from_emd, _get_emd_path
    from ._arcgis_model import _resnet_family, _get_device
    from .._utils.common import (
        get_multispectral_data_params_from_emd,
        _get_emd_path,
    )
    from ._arcgis_model import _resnet_family, _get_device
    from ._ssd_utils import AveragePrecision

    HAS_FASTAI = True

except Exception as e:
    HAS_FASTAI = False


class RTDetrV2Config:

    try:
        import torch
    except:
        pass

    def get_model(self, data, backbone=None, **kwargs):
        from arcgis.learn.models._rtdetr_utils import RTDETR

        # for resizing img during training
        self.scale_factor = kwargs.get("scale_factor", 1)
        self.model = RTDETR(data, backbone, **kwargs)
        # default model parameters
        self.thres = 0.2

        return self.model

    def on_batch_begin(self, learn, model_input_batch, model_target_batch, **kwargs):
        target_list = []

        for bbox, label in zip(*model_target_batch):
            bbox = (bbox + 1) / 2  # change boxes in the range of 0-1.
            # convert to bboxes [xmin,ymin,xmax,ymax].
            bbox = self.torch.index_select(
                bbox,
                1,
                self.torch.tensor([1, 0, 3, 2]).to(learn.data.device),
            )
            mask = (bbox[:, 2:] > (bbox[:, :2])).all(1)
            bbox = bbox[mask]
            label = label[mask]

            def box_xyxy_to_cxcywh(x):
                x0, y0, x1, y1 = x.unbind(-1)
                b = [(x0 + x1) / 2, (y0 + y1) / 2, (x1 - x0), (y1 - y0)]
                return self.torch.stack(b, dim=-1)

            target = {}
            target["boxes"] = box_xyxy_to_cxcywh(bbox)
            target["labels"] = label
            target_list.append(target)
        input_list = [self.resize_input_batch(model_input_batch), target_list]
        return (
            input_list,
            model_target_batch,
        )

    def resize_input_batch(self, input):
        input = self.torch.nn.functional.interpolate(
            input,
            scale_factor=self.scale_factor,
            mode="bilinear",
            recompute_scale_factor=True,
            align_corners=False,
        )

        return input

    def transform_input(self, xb, thresh=0.5, nms_overlap=0.1):
        self.thres = thresh
        return self.resize_input_batch(xb)

    def transform_input_multispectral(self, xb, thresh=0.5, nms_overlap=0.1):
        self.thres = thresh
        return self.resize_input_batch(xb)

    def loss(self, model_output, *model_target):
        loss_dict = self.model.criterion(model_output, model_output["targets"])
        losses = sum(loss_dict.values())
        return losses

    def post_process(
        self, pred, nms_overlap, thres, chip_size, device=torch.device("cuda")
    ):
        post_processed_pred = []
        pred_logits = pred["pred_logits"]
        results = self.model.postprocessors(
            pred,
            (self.torch.ones((pred_logits.shape[0], 2)) * chip_size).to(
                pred_logits.device
            ),
        )
        for result in results:
            bbox, label, score = result["boxes"], result["labels"], result["scores"]
            bbox = bbox / (chip_size / 2) - 1
            kip_pred = score > self.thres
            score = score[kip_pred]
            bbox = bbox[kip_pred]
            label = label[kip_pred]
            # convert bboxes in format [y1,x1,y2,x2]
            bbox = self.torch.index_select(
                bbox, 1, self.torch.tensor([1, 0, 3, 2]).to(bbox.device)
            )
            # Append the tuple in list for each image
            post_processed_pred.append(
                (bbox.data.to(device), label.to(device), score.to(device))
            )

        return post_processed_pred


class RTDetrV2(ModelExtension):
    """
    Model architecture from https://arxiv.org/pdf/2407.17140.
    Creates a ``RTDetrV2`` object detection model,
    based on https://github.com/lyuwenyu/RT-DETR/tree/main.

    =============================   =============================================
    **Parameter**                    **Description**
    -----------------------------   ---------------------------------------------
    data                            Required fastai Databunch. Returned data object from
                                    :meth:`~arcgis.learn.prepare_data`  function.
    -----------------------------   ---------------------------------------------
    backbone                        Optional string. Backbone convolutional neural network
                                    model used for feature extraction, which
                                    is `resnet50` by default.
                                    Supported backbones: ResNet family and specified Timm
                                    models(experimental support) from :func:`~arcgis.learn.RTDetrV2.backbones`.
    -----------------------------   ---------------------------------------------
    pretrained_path                 Optional string. Path where pre-trained model is
                                    saved.
    =============================   =============================================

    :return:
        :class:`~arcgis.learn.RTDetrV2` Object

    """

    def __init__(self, data, backbone="resnet18", pretrained_path=None, **kwargs):
        self._check_dataset_support(data)
        backbone_name = backbone if type(backbone) is str else backbone.__name__
        if backbone_name not in self.supported_backbones:
            raise Exception(
                f"Enter only compatible backbones from {', '.join(self.supported_backbones)}"
            )
        super().__init__(
            data,
            RTDetrV2Config,
            backbone,
            pretrained_path,
            **kwargs,
        )

        self.learn.metrics = [AveragePrecision(self, data.c - 1)]
        idx = self._freeze()
        self.learn.layer_groups = split_model_idx(self.learn.model, [idx])
        self.learn.create_opt(lr=3e-3)

    def unfreeze(self):
        for _, param in self.learn.model.named_parameters():
            param.requires_grad = True

    def _freeze(self):
        backbone = self.learn.model.backbone
        layers = flatten_model(backbone)
        idx = len(layers)
        start_idx = 0
        if self._is_multispectral:
            start_idx = 1
        for layer in layers[start_idx:idx]:
            if (
                isinstance(layer, (torch.nn.BatchNorm2d))
                or isinstance(layer, (fastai.torch_core.ParameterModule))
                or isinstance(layer, (torch.nn.BatchNorm1d))
                or isinstance(layer, (torch.nn.LayerNorm))
            ):
                continue
            for p in layer.parameters():
                p.requires_grad = False
        return idx

    def lr_find(self, allow_plot=True, mixed_precision=False):
        """
        Runs the Learning Rate Finder. Helps in choosing the
        optimum learning rate for training the model.

        =====================   ===========================================
        **Parameter**            **Description**
        ---------------------   -------------------------------------------
        allow_plot              Optional boolean. Display the plot of losses
                                against the learning rates and mark the optimal
                                value of the learning rate on the plot.
                                The default value is 'True'.
        ---------------------   -------------------------------------------
        mixed_precision         Optional boolean. Parameter to enable/disable mixed precision.
                                If set to `True`, optimum learning rate will be derived in mixed precision mode.
                                Only `Pytorch` based models are supported.
                                The default value is 'False'.
        =====================   ===========================================
        """
        lr = super().lr_find(
            allow_plot, start_lr=1e-5, end_lr=3e-4, mixed_precision=mixed_precision
        )
        return lr

    @staticmethod
    def _available_metrics():
        return ["valid_loss", "average_precision"]

    @property
    def supported_backbones(self):
        """Supported list of backbones for this model."""
        return RTDetrV2._supported_backbones()

    @staticmethod
    def backbones():
        """Supported list of backbones for this model."""
        return RTDetrV2._supported_backbones()

    @staticmethod
    def _supported_backbones():
        return [*_resnet_family][:-1]

    @property
    def supported_datasets(self):
        """Supported dataset types for this model."""
        return RTDetrV2._supported_datasets()

    @staticmethod
    def _supported_datasets():
        return ["PASCAL_VOC_rectangles", "KITTI_rectangles"]

    def _save_pytorch_onnx(self, name):
        from arcgis.learn.models._rtdetr_utils import (
            RTDetrDeployWrapper,
            RTDETRPostProcessor,
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            import onnx
        save_path_onnx = str(self.learn.path / self.learn.model_dir / f"{name}.onnx")
        device = self._device
        if hasattr(self._data, "chip_size"):
            chip_size = self._data.chip_size
            if not isinstance(chip_size, tuple):
                chip_size = (chip_size, chip_size)
        num_input_channels = list(self.learn.model.parameters())[0].shape[1]

        data = torch.rand(1, num_input_channels, chip_size[0], chip_size[1]).to(device)
        size = torch.tensor([[chip_size[0], chip_size[1]]]).to(device)

        post_processor = RTDETRPostProcessor(num_classes=self._data.c)
        import copy

        model = RTDetrDeployWrapper(copy.deepcopy(self.learn.model), post_processor)
        model.to(device)
        _ = model(data, size)

        dynamic_axes = {
            "images": {
                0: "N",
            },
            "orig_target_sizes": {0: "N"},
        }

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            torch.onnx.export(
                model,
                (data, size),
                save_path_onnx,
                export_params=True,
                verbose=True,
                input_names=["images", "orig_target_sizes"],
                output_names=["labels", "boxes", "scores"],
                dynamic_axes=dynamic_axes,
                opset_version=16,
                do_constant_folding=True,
            )
        save_path_onnx = f"{name}.onnx"
        return [save_path_onnx]

    @classmethod
    def from_model(cls, emd_path, data=None):
        """
        Creates a ``RTDetrV2`` object from an Esri Model Definition (EMD) file.

        =====================   ===========================================
        **Parameter**            **Description**
        ---------------------   -------------------------------------------
        emd_path                Required string. Path to Deep Learning Package
                                (DLPK) or Esri Model Definition(EMD) file.
        ---------------------   -------------------------------------------
        data                    Required fastai Databunch or None. Returned data
                                object from :meth:`~arcgis.learn.prepare_data`  function or None for
                                inferencing.

        =====================   ===========================================

        :return:
            :class:`~arcgis.learn.RTDetrV2` Object
        """
        emd_path = _get_emd_path(emd_path)

        with open(emd_path) as f:
            emd = json.load(f)

        model_file = Path(emd["ModelFile"])

        if not model_file.is_absolute():
            model_file = emd_path.parent / model_file

        backbone = emd["ModelParameters"]["backbone"]
        dataset_type = emd.get("DatasetType", "PASCAL_VOC_rectangles")
        chip_size = emd["ImageWidth"]
        resize_to = emd.get("resize_to", None)
        kwargs = emd.get("Kwargs", {})
        if isinstance(resize_to, list):
            resize_to = (resize_to[0], resize_to[1])

        try:
            class_mapping = {i["Value"]: i["Name"] for i in emd["Classes"]}
            color_mapping = {i["Value"]: i["Color"] for i in emd["Classes"]}
        except KeyError:
            class_mapping = {i["ClassValue"]: i["ClassName"] for i in emd["Classes"]}
            color_mapping = {i["ClassValue"]: i["Color"] for i in emd["Classes"]}

        data_passed = True
        if data is None:
            data_passed = False
            train_tfms = []
            val_tfms = []
            ds_tfms = (train_tfms, val_tfms)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                sd = ImageList([], path=emd_path.parent.parent.parent).split_by_idx([])
                data = (
                    sd.label_const(
                        0,
                        label_cls=ObjectDetectionCategoryList,
                        classes=list(class_mapping.values()),
                    )
                    .transform(ds_tfms)
                    .databunch(device=_get_device())
                    .normalize(imagenet_stats)
                )
            # Add 1 for background class
            data.c += 1
            data.chip_size = chip_size
            data.class_mapping = class_mapping
            data.color_mapping = color_mapping
            data.classes = ["background"] + list(class_mapping.values())
            data._is_empty = True
            data.emd_path = emd_path
            data.emd = emd
            data = get_multispectral_data_params_from_emd(data, emd)
            data.dataset_type = dataset_type

        data.resize_to = resize_to
        rtdetr = cls(data, backbone, pretrained_path=str(model_file), **kwargs)

        if not data_passed:
            rtdetr.learn.data.single_ds.classes = rtdetr._data.classes
            rtdetr.learn.data.single_ds.y.classes = rtdetr._data.classes

        rtdetr._model_emd = emd
        return rtdetr

    def predict(
        self,
        image_path,
        threshold=0.5,
        nms_overlap=0.1,
        return_scores=False,
        visualize=False,
        resize=False,
    ):
        """
        Runs prediction on an Image. This method is only supported for RGB images.

        =====================   ===========================================
        **Parameter**            **Description**
        ---------------------   -------------------------------------------
        image_path              Required. Path to the image file to make the
                                predictions on.
        ---------------------   -------------------------------------------
        threshold               Optional float. The probability above which
                                a detection will be considered valid.
        ---------------------   -------------------------------------------
        nms_overlap             Optional float. The intersection over union
                                threshold with other predicted bounding
                                boxes, above which the box with the highest
                                score will be considered a true positive.
        ---------------------   -------------------------------------------
        return_scores           Optional boolean. Will return the probability
                                scores of the bounding box predictions if True.
        ---------------------   -------------------------------------------
        visualize               Optional boolean. Displays the image with
                                predicted bounding boxes if True.
        ---------------------   -------------------------------------------
        resize                  Optional boolean. Resizes the image to the
                                same size (chip_size parameter in prepare_data)
                                that the model was trained on, before detecting
                                objects. Note that if resize_to parameter was
                                used in prepare_data, the image is resized to
                                that size instead.

                                By default, this parameter is false and the
                                detections are run in a sliding window fashion
                                by applying the model on cropped sections of
                                the image (of the same size as the model was
                                trained on).
        =====================   ===========================================

        :return: Returns a tuple with predictions, labels and optionally confidence scores
                  if return_scores=True. The predicted bounding boxes are returned as a list
                  of lists containing the  xmin, ymin, width and height of each predicted object
                  in each image. The labels are returned as a list of class values and the
                  confidence scores are returned as a list of floats indicating the confidence
                  of each prediction.
        """

    def predict_video(
        self,
        input_video_path,
        metadata_file,
        threshold=0.5,
        nms_overlap=0.1,
        track=False,
        visualize=False,
        output_file_path=None,
        multiplex=False,
        multiplex_file_path=None,
        tracker_options={
            "assignment_iou_thrd": 0.3,
            "vanish_frames": 40,
            "detect_frames": 10,
        },
        visual_options={
            "show_scores": True,
            "show_labels": True,
            "thickness": 2,
            "fontface": 0,
            "color": (255, 255, 255),
        },
        resize=False,
    ):
        """
        Runs prediction on a video and appends the output VMTI predictions in the metadata file.
        This method is only supported for RGB images.

        =====================   ===========================================
        **Parameter**            **Description**
        ---------------------   -------------------------------------------
        input_video_path        Required. Path to the video file to make the
                                predictions on.
        ---------------------   -------------------------------------------
        metadata_file           Required. Path to the metadata csv file where
                                the predictions will be saved in VMTI format.
        ---------------------   -------------------------------------------
        threshold               Optional float. The probability above which
                                a detection will be considered.
        ---------------------   -------------------------------------------
        nms_overlap             Optional float. The intersection over union
                                threshold with other predicted bounding
                                boxes, above which the box with the highest
                                score will be considered a true positive.
        ---------------------   -------------------------------------------
        track                   Optional bool. Set this parameter as True to
                                enable object tracking.
        ---------------------   -------------------------------------------
        visualize               Optional boolean. If True a video is saved
                                with prediction results.
        ---------------------   -------------------------------------------
        output_file_path        Optional path. Path of the final video to be saved.
                                If not supplied, video will be saved at path input_video_path
                                appended with _prediction.
        ---------------------   -------------------------------------------
        multiplex               Optional boolean. Runs Multiplex using the VMTI detections.
        ---------------------   -------------------------------------------
        multiplex_file_path     Optional path. Path of the multiplexed video to be saved.
                                By default a new file with _multiplex.MOV extension is saved
                                in the same folder.
        ---------------------   -------------------------------------------
        tracking_options        Optional dictionary. Set different parameters for
                                object tracking. assignment_iou_thrd parameter is used
                                to assign threshold for assignment of trackers,
                                vanish_frames is the number of frames the object should
                                be absent to consider it as vanished, detect_frames
                                is the number of frames an object should be detected
                                to track it.
        ---------------------   -------------------------------------------
        visual_options          Optional dictionary. Set different parameters for
                                visualization.
                                show_scores boolean, to view scores on predictions,
                                show_labels boolean, to view labels on predictions,
                                thickness integer, to set the thickness level of box,
                                fontface integer, fontface value from opencv values,
                                color tuple (B, G, R), tuple containing values between
                                0-255.
        ---------------------   -------------------------------------------
        resize                  Optional boolean. Resizes the video frames to the same size
                                (chip_size parameter in prepare_data) that the model was
                                trained on, before detecting objects. Note that if
                                resize_to parameter was used in prepare_data,
                                the video frames are resized to that size instead.

                                By default, this parameter is false and the detections
                                are run in a sliding window fashion by applying the
                                model on cropped sections of the frame (of the same
                                size as the model was trained on).
        =====================   ===========================================

        """

    def average_precision_score(
        self,
        detect_thresh=0.2,
        iou_thresh=0.1,
        mean=False,
        show_progress=True,
    ):
        """
        Computes average precision on the validation set for each class.

        =====================   ===========================================
        **Parameter**            **Description**
        ---------------------   -------------------------------------------
        detect_thresh           Optional float. The probability above which
                                a detection will be considered for computing
                                average precision.
        ---------------------   -------------------------------------------
        iou_thresh              Optional float. The intersection over union
                                threshold with the ground truth labels, above
                                which a predicted bounding box will be
                                considered a true positive.
        ---------------------   -------------------------------------------
        mean                    Optional bool. If False returns class-wise
                                average precision otherwise returns mean
                                average precision.
        =====================   ===========================================

        :return: `dict` if mean is False otherwise `float`
        """

    def show_results(self, rows=5, thresh=0.5, nms_overlap=0.1):
        """
        Displays the results of a trained model on a part of the validation set.

        =====================   ===========================================
        **Parameter**            **Description**
        ---------------------   -------------------------------------------
        rows                    Optional int. Number of rows of results
                                to be displayed.
        ---------------------   -------------------------------------------
        thresh                  Optional float. The probability above which
                                a detection will be considered valid.
        ---------------------   -------------------------------------------
        nms_overlap             Optional float. The intersection over union
                                threshold with other predicted bounding
                                boxes, above which the box with the highest
                                score will be considered a true positive.
        =====================   ===========================================

        """
