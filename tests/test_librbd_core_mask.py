from control.cluster import default_librbd_core_mask, resolve_librbd_core_mask


def test_default_librbd_core_mask_after_reactor_cores():
    assert default_librbd_core_mask(8) == "[4-7]"
    assert default_librbd_core_mask(16) == "[4-7]"


def test_default_librbd_core_mask_needs_eight_cpus():
    assert default_librbd_core_mask(7) is None
    assert default_librbd_core_mask(4) is None
    assert default_librbd_core_mask(None) is None


def test_explicit_librbd_core_mask_is_kept():
    assert resolve_librbd_core_mask("0xF0", 16) == "0xF0"
    assert resolve_librbd_core_mask(None, 16) == "[4-7]"
    assert resolve_librbd_core_mask(None, 4) is None
