import adler32

def ECRC(buf, int_val):
    val = 1L * int_val
    return adler32.adler32(val, buf, len(buf))
