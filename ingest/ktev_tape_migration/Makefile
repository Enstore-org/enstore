all: copy_tape
copy_tape: copy_tape.c
	$(CC) -I. -I$(FTT_DIR)/ftt_lib -o copy_tape copy_tape.c \
	-L. -L$(FTT_DIR)/lib -L$(FTT_DIR)/ftt_lib -lftt_test -lftt $(LIBDS)
