try:
    from addict import Dict
    import types
    import torch
    from torch import nn
    import spconv.pytorch as spconv
    import torch.nn.functional as F
    from mmdet3d.structures import LiDARInstance3DBoxes
    from mmengine.structures import InstanceData
    from mmdet3d.models.voxel_encoders import HardSimpleVFE
    from mmdet3d.models.backbones import SECOND
    from mmdet3d.models.necks import SECONDFPN
    from mmdet3d.registry import MODELS
    from ._mmdet3d_utils import forward_neck, model_data_preprocessor
    from ._arcgis_model import _EmptyData
    from ._point_transformerv3_utils import PointSequential, PTV3Backbone
    from .._utils.pointcloud_serialization import PointBatchPreprocess as Point

    HAS_FASTAI = True
except Exception as e:
    HAS_FASTAI = False


class PointTransformerV3Det(nn.Module):
    def __init__(self, data, data_preprocessor, **kwargs):

        super().__init__()
        self.data_preprocessor = data_preprocessor
        self.sparse_shape = data.voxel_sparse_shape
        self.voxel_encoder = HardSimpleVFE(num_features=data.num_features)
        self.middle_encoder = MiddleEncoder(
            data, sparse_shape=self.sparse_shape, **kwargs
        ).to(data.device)
        backbone_inchannels = self.midele_out_channs(data)
        self.backbone = SECOND(
            backbone_inchannels,
            layer_nums=[3, 3],
            layer_strides=[1, 2],
            out_channels=[128, 256],
        )
        self.neck = SECONDFPN(
            in_channels=[128, 256],
            upsample_strides=[1, 2],
            out_channels=[256, 256],
        )
        self.neck.forward = types.MethodType(forward_neck, self.neck)
        self._box_type = kwargs.get("head_type", "CenterHead")
        if self._box_type is "CenterHead":
            self.bbox_head = bbox_center(data)
            self._feature_map_size = (
                torch.tensor(self.bbox_head.train_cfg.grid_size[:2])
                // self.bbox_head.train_cfg.out_size_factor
            ).tolist()
        else:
            self.bbox_head = bbox_anchor(data)
        self.prediction = False

    def midele_out_channs(self, data):
        x_batch = torch.rand(
            (10000, data.num_features), dtype=torch.float32, device=data.device
        )
        x_batch[:, :3] = (x_batch[:, :3] - 0.5) / 0.5
        x_batch[:, :3] *= data.scale_factor
        voxel_dict = self.data_preprocessor.voxelize([x_batch], data_samples=None)
        middle_feature = self(dict(inputs=dict(voxels=voxel_dict)), middle_feature=True)

        return middle_feature.shape[1]

    def ptv3formate(self, voxel_features, voxel_centers, voxel_coors, sparse_shape):
        input_dict = {}
        input_dict["feat"] = voxel_features
        input_dict["coord"] = voxel_centers
        input_dict["grid_coord"] = voxel_coors[:, 1:]
        input_dict["batch"] = voxel_coors[:, 0]
        input_dict["sparse_shape"] = sparse_shape

        point = Point(input_dict)

        return point

    def forward(self, batch_inputs_dict, middle_feature=False):
        voxel_dict = batch_inputs_dict["inputs"]["voxels"]
        voxel_features = self.voxel_encoder(
            voxel_dict["voxels"], voxel_dict["num_points"], voxel_dict["coors"]
        )
        point = self.ptv3formate(
            voxel_features,
            voxel_dict["voxel_centers"],
            voxel_dict["coors"],
            self.sparse_shape,
        )

        point = self.middle_encoder(point)
        if middle_feature:
            return point

        point = self.backbone(point)
        point = self.neck(point)

        if getattr(self, "_feature_map_size", False):
            point = [
                F.interpolate(
                    p, self._feature_map_size, mode="bilinear", align_corners=True
                )
                for p in point
            ]

        # return point

        if not self.prediction:
            losses = self.bbox_head.loss(point, batch_inputs_dict["data_samples"])
            losses = self.parse_losses(losses)
            output = None
            if not self.training:
                self.eval()
                results_list = self.bbox_head.predict(
                    point, batch_inputs_dict["data_samples"]
                )
                output = self.add_pred_to_datasample(
                    batch_inputs_dict["data_samples"], results_list
                )
                if self._box_type is "CenterHead":
                    output = self.remove_velocity(output)
            return output, losses
        else:
            results_list = self.bbox_head.predict(
                point, batch_inputs_dict["data_samples"]
            )
            output = self.add_pred_to_datasample(
                batch_inputs_dict["data_samples"], results_list
            )
            if self._box_type is "CenterHead":
                output = self.remove_velocity(output)
            return output

    def remove_velocity(self, output):

        for b in output:
            bboxs = b.pred_instances_3d.bboxes_3d.tensor
            b.pred_instances_3d.bboxes_3d = LiDARInstance3DBoxes(
                bboxs[:, :7], box_dim=7, with_yaw=True
            )
            if hasattr(b.gt_instances_3d, "bboxes_3d"):
                bboxs = b.gt_instances_3d.bboxes_3d.tensor
                b.gt_instances_3d.bboxes_3d = LiDARInstance3DBoxes(
                    bboxs[:, :7], box_dim=7, with_yaw=True
                )
        return output

    def add_pred_to_datasample(
        self,
        data_samples,
        data_instances_3d,
    ):
        if data_instances_3d is None:
            data_instances_3d = [InstanceData() for _ in range(len(data_instances_3d))]
        for i, data_sample in enumerate(data_samples):
            data_sample.pred_instances_3d = data_instances_3d[i]
        return data_samples

    def parse_losses(self, losses):
        if self._box_type is "CenterHead":
            return sum(losses.values())
        log_vars = []
        for loss_name, loss_value in losses.items():
            log_vars.append([loss_name, sum(_loss.mean() for _loss in loss_value)])

        loss = sum(value for key, value in log_vars if "loss" in key)

        return loss


def make_sparse_convmodule(in_channels, indice_key, out_channels=None):
    out_channels = out_channels if out_channels else in_channels
    sparse_module = PointSequential(
        spconv.SparseConv3d(
            in_channels,
            out_channels,
            kernel_size=(3, 1, 1),
            stride=(2, 1, 1),
            padding=0,
            bias=False,
            indice_key=indice_key,
        ),
        nn.BatchNorm1d(out_channels, eps=1e-3, momentum=0.01),
        nn.GELU(),
    )
    return sparse_module


class MiddleEncoder(nn.Module):
    def __init__(self, data, sparse_shape, num_layer=1, **kwargs):
        super().__init__()
        self.encoder = PTV3Backbone(
            in_channels=data.num_features,
            enc_depths=(1, 2, 2, 2),
            enc_channels=(16, 32, 64, 64),
            enc_num_head=(2, 4, 8, 16),
            cls_mode=True,
            is_detection=True,
            **kwargs,
        )
        self.conv_out = PointSequential()
        for l in range(num_layer):
            downsample = make_sparse_convmodule(
                64, indice_key=f"downsample{l}", out_channels=128
            )
            self.conv_out.add(module=downsample, name=f"downsample_zdim{l}")
        self.detection_pooling = kwargs.get("detection_pooling", False)
        if not self.detection_pooling:
            # stride in 2d feature
            stride = 2 ** (self.encoder.num_stages - 1)
            self.out_spatial_shape = list(torch.tensor(sparse_shape) // stride)

    def forward(self, point: Point):
        point = self.encoder(point)
        if not self.detection_pooling:
            point.sparse_conv_feat.spatial_shape = self.out_spatial_shape

        point = self.conv_out(point)

        spatial_features = point.sparse_conv_feat.dense()

        N, C, D, H, W = spatial_features.shape
        spatial_features = spatial_features.view(N, C * D, H, W)
        return spatial_features


def bbox_center(data):
    task_list = []
    for cls_id in sorted(data.idx2class.keys()):
        task_list.append(Dict(num_class=1, class_names=[str(data.idx2class[cls_id])]))

    bbox_head = Dict(
        type="CenterHead",
        in_channels=sum([256, 256]),
        tasks=task_list,
        common_heads=Dict(
            reg=(2, 2), height=(1, 2), dim=(3, 2), rot=(2, 2), vel=(2, 2)
        ),
        share_conv_channel=64,
        bbox_coder=Dict(
            type="CenterPointBBoxCoder",
            post_center_range=data.range,
            max_num=100,
            score_threshold=0.1,
            out_size_factor=8,
            voxel_size=data.voxel_size[:2],
            code_size=9,
            pc_range=data.range,
        ),
        separate_head=Dict(type="SeparateHead", init_bias=-2.19, final_kernel=3),
        loss_cls=Dict(type="mmdet.GaussianFocalLoss", reduction="mean"),
        loss_bbox=Dict(type="mmdet.L1Loss", reduction="mean", loss_weight=0.25),
        norm_bbox=True,
    )
    train_cfg = Dict(
        pts=Dict(
            grid_size=data.voxel_sparse_shape[::-1],
            voxel_size=data.voxel_size[:2],
            out_size_factor=8,  # downsample factor from satrt to final feature_map
            dense_reg=1,
            gaussian_overlap=0.1,
            max_objs=100,  # maximum number of object in an tile to calculate loss
            min_radius=2,
            code_weights=[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0],
            point_cloud_range=data.range,
        )
    )

    test_cfg = Dict(
        pts=Dict(
            post_center_limit_range=data.range,
            score_threshold=0.1,  # min conf score of boxes
            out_size_factor=8,  # downsample factor from satrt to final feature_map
            voxel_size=data.voxel_size[:2],
            nms_type="rotate",
            pre_max_size=1000,  # Max no of boxes to keep before NMS on the basis of sorted box confidence
            post_max_size=83,  # Max no of boxes to keep after NMS on the basis of sorted box confidence
            nms_thr=0.2,  # nms thres for detection
            pc_range=data.range,
        )
    )
    bbox_head.update(train_cfg=train_cfg.pts)
    bbox_head.update(test_cfg=test_cfg.pts)

    bbox_head = MODELS.build(bbox_head)
    return bbox_head


def bbox_anchor(data):
    bbox_head = Dict(
        type="Anchor3DHead",
        num_classes=1,
        in_channels=512,
        feat_channels=512,
        use_direction_classifier=True,
        anchor_generator=Dict(
            type="Anchor3DRangeGenerator",
            ranges=[[0, -40.0, -1.78, 70.4, 40.0, -1.78]],
            sizes=[[3.9, 1.6, 1.56]],
            rotations=[0, 1.57],
            reshape_out=True,
        ),
        diff_rad_by_sin=True,
        bbox_coder=Dict(type="DeltaXYZWLHRBBoxCoder"),
        loss_cls=Dict(
            type="mmdet.FocalLoss",
            use_sigmoid=True,
            gamma=2.0,
            alpha=0.25,
            loss_weight=1.0,
        ),
        loss_bbox=Dict(type="mmdet.SmoothL1Loss", beta=1.0 / 9.0, loss_weight=2.0),
        loss_dir=Dict(
            type="mmdet.CrossEntropyLoss", use_sigmoid=False, loss_weight=0.2
        ),
    )
    # model training and testing settings
    train_cfg = Dict(
        assigner=Dict(
            type="Max3DIoUAssigner",
            iou_calculator=Dict(type="BboxOverlapsNearest3D"),
            pos_iou_thr=0.6,
            neg_iou_thr=0.45,
            min_pos_iou=0.45,
            ignore_iof_thr=-1,
        ),
        allowed_border=0,
        pos_weight=-1,
        debug=False,
    )
    test_cfg = Dict(
        use_rotate_nms=True,
        nms_across_levels=False,
        nms_thr=0.01,
        score_thr=0.1,
        min_bbox_size=0,
        nms_pre=100,
        max_num=50,
    )

    bbox_head.num_classes = data.c
    bbox_head.bbox_coder.code_size = 7
    bbox_head.anchor_generator.ranges = data.anchor_range
    bbox_head.anchor_generator.sizes = data.average_box_size
    bbox_head.anchor_generator.rotations = [0.0, 1.57]
    bbox_head.update(train_cfg=train_cfg)
    bbox_head.update(test_cfg=test_cfg)

    return MODELS.build(bbox_head)


def prep_data_preprocessor(data, **kwargs):

    preprocessor_cfg = Dict(
        type="Det3DDataPreprocessor",
        voxel=True,
        voxel_layer=Dict(),
    )

    data_preprocessor = model_data_preprocessor(preprocessor_cfg, data, **kwargs).to(
        data.device
    )

    if isinstance(data, _EmptyData):
        data.data_preprocessor_3d = data_preprocessor
    else:
        if kwargs.get("head_type", "CenterHead") == "CenterHead":
            data.train_dl._add_velocity = True
            data.valid_dl._add_velocity = True
        data.train_dl.data_preprocessor_3d = data_preprocessor
        data.valid_dl.data_preprocessor_3d = data_preprocessor

    return data, data_preprocessor
