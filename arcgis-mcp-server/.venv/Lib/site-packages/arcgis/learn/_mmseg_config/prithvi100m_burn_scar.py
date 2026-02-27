# required bands Blue, Green, Red, Narrow NIR, SWIR 1, SWIR 2
bands = [0, 1, 2, 3, 4, 5]

CLASSES = ("Unburnt land", "Burn scar")

img_norm_burn_model = dict(
    means=[
        0.033349706741586264,
        0.05701185520536176,
        0.05889748132001316,
        0.2323245113436119,
        0.1972854853760658,
        0.11944914225186566,
    ],
    stds=[
        0.02269135568823774,
        0.026807560223070237,
        0.04004109844362779,
        0.07791732423672691,
        0.08708738838140137,
        0.07241979477437814,
    ],
)
nframes = 1
norm_cfg = dict(type="BN", requires_grad=True)
model = dict(
    type="EncoderDecoder",
    backbone=dict(
        type="PrithviBackbone",
        img_size=224,
        in_chans=len(bands),
        num_frames=nframes,
        tubelet_size=1,
        pretrained=False,
    ),
    neck=dict(
        type="PrithviNeck",
        embed_dim=768 * nframes,
        output_embed_dim=768 * nframes,
        input_hw=(14, 14),
    ),
    decode_head=dict(
        in_channels=768 * nframes,
        type="FCNHead",
        in_index=-1,
        channels=256,
        num_convs=1,
        concat_input=False,
        dropout_ratio=0.1,
        norm_cfg=dict(type="BN", requires_grad=True),
        align_corners=False,
        loss_decode=dict(
            type="DiceLoss", use_sigmoid=False, loss_weight=1, ignore_index=-1
        ),
    ),
    auxiliary_head=dict(
        in_channels=768 * nframes,
        type="FCNHead",
        in_index=-1,
        channels=256,
        num_convs=2,
        concat_input=False,
        dropout_ratio=0.1,
        norm_cfg=dict(type="BN", requires_grad=True),
        align_corners=False,
        loss_decode=dict(
            type="DiceLoss", use_sigmoid=False, loss_weight=1, ignore_index=-1
        ),
    ),
    # model training and testing settings
    train_cfg=dict(),
    test_cfg=dict(mode="whole"),
)

checkpoint = "https://huggingface.co/ibm-nasa-geospatial/Prithvi-100M-burn-scar/resolve/main/burn_scars_Prithvi_100M.pth"
