
extern short int *__xtype_b;
__asm__(".symver __xtype_b,__ctype_b@GLIBC_2.0");

short int *__ctype_b;

hack_init() {
__ctype_b = __xtype_b;
}
