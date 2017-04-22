import ctypes

_dset = ctypes.CDLL('libsum.so')
_dset.unite.argtypes = (ctypes.c_uint32, ctypes.c_uint32)
_dset.unite.restype = ctypes.c_uint32

# uintptr_t
def unite(u, v):
    global _dset
    result = _dset.unite(ctypes.c_uint32(u), ctypes.c_uint32(v))
    return result

unite(1, 6)
unite(2, 6)