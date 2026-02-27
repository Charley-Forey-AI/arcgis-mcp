bands = [0, 1, 2, 3, 4, 5]
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
        pretrained=True,
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
        loss_decode=dict(type="CrossEntropyLoss", use_sigmoid=False, loss_weight=0.4),
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
        loss_decode=dict(type="CrossEntropyLoss", use_sigmoid=False, loss_weight=0.4),
    ),
    # model training and testing settings
    train_cfg=dict(),
    test_cfg=dict(mode="whole"),
)
