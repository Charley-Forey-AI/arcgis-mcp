bands = [1, 2, 3, 8, 11, 12]
# required bands Blue, Green, Red, Narrow NIR, SWIR 1, SWIR 2

CLASSES = ("flooded", "non-flooded")

img_norm_flood_model = dict(
    means=[0.14245495, 0.13921481, 0.12434631, 0.31420089, 0.20743526, 0.12046503],
    stds=[0.04036231, 0.04186983, 0.05267646, 0.0822221, 0.06834774, 0.05294205],
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
        num_classes=2,
        in_channels=768 * nframes,
        ignore_index=2,
        type="FCNHead",
        in_index=-1,
        channels=256,
        num_convs=1,
        concat_input=False,
        dropout_ratio=0.1,
        norm_cfg=dict(type="BN", requires_grad=True),
        align_corners=False,
        loss_decode=dict(
            type="CrossEntropyLoss",
            use_sigmoid=False,
            loss_weight=1,
            class_weight=[0.3, 0.7, 0],
        ),
    ),
    auxiliary_head=dict(
        num_classes=2,
        in_channels=768 * nframes,
        ignore_index=2,
        type="FCNHead",
        in_index=-1,
        channels=256,
        num_convs=2,
        concat_input=False,
        dropout_ratio=0.1,
        norm_cfg=dict(type="BN", requires_grad=True),
        align_corners=False,
        loss_decode=dict(
            type="CrossEntropyLoss",
            use_sigmoid=False,
            loss_weight=1,
            class_weight=[0.3, 0.7, 0],
        ),
    ),
    # model training and testing settings
    train_cfg=dict(),
    test_cfg=dict(mode="whole"),
)

checkpoint = "https://huggingface.co/ibm-nasa-geospatial/Prithvi-100M-sen1floods11/resolve/main/sen1floods11_Prithvi_100M.pth"
