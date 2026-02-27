import torch
import matplotlib.pyplot as plt
import math
import numpy as np
from .common import get_nbatches, image_batch_stretcher
from .._utils.env import is_arcgispronotebook


def show_batch_labeled_tiles(self, rows=3, **kwargs):  # parameters adjusted in kwargs
    """
    This function randomly picks a few training chips and visualizes them.

    =====================   ===========================================
    **Parameter**            **Description**
    ---------------------   -------------------------------------------
    rows                    Optional Integer.
                            Number of rows to display.
                            Default: 3.
    ---------------------   -------------------------------------------
    alpha                   Optional Float.
                            Opacity of the lables for the corresponding
                            images. Values range between 0 and 1, where
                            1 means opaque.
    -------------------------------------------------------------------

    """
    from .._utils.common import denorm_x

    nrows = rows
    ncols = kwargs.get("ncols", nrows)
    # start_index = kwargs.get('start_index', 0) # Does not work with dataloader

    # Modify nrows and ncols according to the dataset
    n_items = kwargs.get("n_items", nrows * ncols)
    n_items = min(n_items, len(self.x))
    nrows = math.ceil(n_items / ncols)
    nbatches = math.ceil(n_items / self.batch_size)

    type_data_loader = kwargs.get(
        "data_loader", "training"
    )  # options : traininig, validation, testing
    if type_data_loader == "training":
        data_loader = self.train_dl
    elif type_data_loader == "validation":
        data_loader = self.valid_dl
    elif type_data_loader == "testing":
        data_loader = self.test_dl
    else:
        e = Exception(
            f"could not find {type_data_loader} in data. Please ensure that the data loader type is traininig, validation or testing "
        )
        raise (e)

    rgb_bands = kwargs.get("rgb_bands", self._symbology_rgb_bands)
    nodata = kwargs.get("nodata", 0)
    imsize = kwargs.get("imsize", 5)
    statistics_type = kwargs.get(
        "statistics_type", "dataset"
    )  # Accepted Values `dataset`, `DRA`
    stretch_type = kwargs.get(
        "stretch_type", "minmax"
    )  # Accepted Values `minmax`, `percentclip`

    e = Exception(
        "`rgb_bands` should be a valid band_order, list or tuple of length 3 or 1."
    )
    symbology_bands = []
    if not (len(rgb_bands) == 3 or len(rgb_bands) == 1):
        raise (e)
    for b in rgb_bands:
        if type(b) == str:
            b_index = self._bands.index(b)
        elif type(b) == int:
            self._bands[
                b
            ]  # To check if the band index specified by the user really exists.
            b_index = b
        else:
            raise (e)
        b_index = self._extract_bands.index(b_index)
        symbology_bands.append(b_index)

    # Get Batch
    x_batch, y_batch = get_nbatches(data_loader, nbatches)
    x_batch = torch.cat(x_batch)
    y_batch = torch.cat(y_batch)

    # Denormalize X
    x_batch = denorm_x(x_batch, self)

    # Extract RGB Bands
    symbology_x_batch = x_batch[:, symbology_bands]
    if stretch_type is not None:
        symbology_x_batch = image_batch_stretcher(
            symbology_x_batch, stretch_type, statistics_type
        )

    # Channel first to channel last and clamp float values to range 0 - 1 for plotting
    symbology_x_batch = symbology_x_batch.permute(0, 2, 3, 1)
    # Clamp float values to range 0 - 1
    if symbology_x_batch.mean() < 1:
        symbology_x_batch = symbology_x_batch.clamp(0, 1)

    # Squeeze channels if single channel (1, 224, 224) -> (224, 224)
    if symbology_x_batch.shape[-1] == 1:
        symbology_x_batch = symbology_x_batch.squeeze()

    # Get color Array
    color_array = self._multispectral_color_array

    # Size for plotting
    fig, axs = plt.subplots(
        nrows=nrows, ncols=ncols, figsize=(ncols * imsize, nrows * imsize)
    )
    idx = 0
    for r in range(nrows):
        for c in range(ncols):
            if idx < symbology_x_batch.shape[0]:
                axi = axs
                if nrows == 1:
                    axi = axi
                else:
                    axi = axi[r]
                if ncols == 1:
                    axi = axi
                else:
                    axi = axi[c]
                axi.imshow(symbology_x_batch[idx].cpu().numpy())

                if self.dataset_type == "MultiLabeled_Tiles":
                    one_hot_labels = y_batch[idx].tolist()
                    from itertools import compress

                    labels = compress(self.classes, one_hot_labels)
                    title = ";".join(labels)
                else:
                    title = f"{self.classes[y_batch[idx].item()]}"

                axi.set_title(title)
                axi.axis("off")
            else:
                axs[r][c].axis("off")
            idx += 1
    if is_arcgispronotebook():
        plt.show()


# Function to plot hard examples for multilabel classification
# This function has been taken from fastai and modified to work with multispectral and rgb (ArcGISMSImage).
def plot_multi_top_losses_modified(
    self, samples=3, figsize=(8, 8), save_misclassified=False
):
    "Show images in `top_losses` along with their prediction, actual, loss, and probability of predicted class in a multilabeled dataset."

    # Get predicted probabilities as a 2D tensor with shape (num_valid_img, num_classes), ground truth labels 2-D, and corresponding losses from the model 1D num_class*num_valid_img
    probability_per_class, truthlabels, losses = self.learn.get_preds(with_loss=True)

    predclass_label = (probability_per_class > 0.5).float()
    num_classes = len(self.data.classes)
    num_valid_img = len(self.data.valid_ds)

    # Create an index list that repeats each image index for the number of classes
    idx_img = [x for x in range(num_valid_img) for _ in range(num_classes)]
    converted_predclass_labels = []
    converted_truthclass_labels = []

    # Convert predicted class labels: store class indices where the prediction is 1, otherwise store None
    for row in predclass_label:
        converted_predclass_labels.append(
            [
                (
                    i if value == 1 else None
                )  # Store class index if predicted, otherwise None
                for i, value in enumerate(row)
            ]
        )

    for row in truthlabels:
        converted_truthclass_labels.append(
            [i if value == 1 else None for i, value in enumerate(row)]
        )

    # Create flattened nested lists
    from itertools import chain

    flattened_predclass_labels = list(chain.from_iterable(converted_predclass_labels))
    flattened_truthclass_labels = list(chain.from_iterable(converted_truthclass_labels))

    combined_list = []

    for i, (truth_labels, pred_labels) in enumerate(
        zip(flattened_truthclass_labels, flattened_predclass_labels)
    ):
        # The below if statement eliminates cases where both truth and prediction are None
        if not ((truth_labels == None) and (pred_labels == None)):

            # Append relevant data to combined_list: [Image ID , Ground truth labels, Predicted labels , Loss value , Predicted probability]
            combined_list.append(
                (
                    idx_img[i],
                    truth_labels,
                    pred_labels,
                    losses[i],
                    probability_per_class.view(-1)[i],
                )
            )

    # This can occur when all predictions and actual labels are None, resulting in an empty combined_list.
    if len(combined_list) == 0:
        print("There are no mismatches in the prediction.")
        return

    # Sort mismatches based on the loss value in descending order
    mismatches = sorted(combined_list, key=lambda x: x[3].item(), reverse=True)

    samples = min(samples, len(mismatches))
    from arcgis.learn._utils.common import ArcGISMSImage
    from itertools import compress

    for sampleN in range(samples):
        imag = (self.data.valid_ds[mismatches[sampleN][0]])[0]
        imag = ArcGISMSImage.show(imag, return_ax=True)
        predicted_idx = mismatches[sampleN][2]
        actual_idx = mismatches[sampleN][1]
        predicted_classes = (
            str(self.data.classes[predicted_idx])
            if predicted_idx is not None
            else f"not predicted as { str(self.data.classes[actual_idx])}"
        )
        actual_classes = (
            str(self.data.classes[actual_idx])
            if actual_idx is not None
            else f"not labelled as {str(self.data.classes[predicted_idx])}"
        )
        imag.set_title(
            f"""Actual: {actual_classes} \nPrediction: {predicted_classes} \nLoss: {mismatches[sampleN][3].numpy()}\nProbability: {mismatches[sampleN][4]}""",
            loc="left",
        )
        plt.show()
        if save_misclassified:
            return mismatches
