import sequence
import ps

sequence.font = ps.Courier(8)

ps.put_header()

p = sequence.Uml_sequence_page("WRITE_ERROR -- drive error writing data block")

encp   = p.add_class_role(sequence.Uml_class_role("Encp"))
lm     = p.add_class_role(sequence.Uml_class_role("LibMan"))
mover  = p.add_class_role(sequence.Uml_class_role("Mover"))
mc     = p.add_class_role(sequence.Uml_class_role("MedChng"))
amu    = p.add_class_role(sequence.Uml_class_role("Amu"))
vc     = p.add_class_role(sequence.Uml_class_role("VClerk"))
fc     = p.add_class_role(sequence.Uml_class_role("FClerk"))
ls     = p.add_class_role(sequence.Uml_class_role("logSvr"))
td     = p.add_class_role(sequence.Uml_class_role("TapeDrv"))
arm    = p.add_class_role(sequence.Uml_class_role("Arm"))
other_encp    = p.add_class_role(sequence.Uml_class_role("other encps"))

p.add_message(sequence.Uml_message(mover,lm,"you summoned me"))
p.add_message(sequence.Uml_message(lm, mover,"read F from V"))
p.add_message(sequence.Uml_message(mover, encp,"Hello"))
p.add_message(sequence.Uml_message (encp, mover, "port= ip="))


p.add_message(sequence.Uml_message (mover, td, 
		"(FTT) precautionay offline and eject ?cleaning tape?"))
p.add_message(sequence.Uml_message (mover, mc, "loadvol V"))
p.add_message(sequence.Uml_message (mc, amu, "DISMOUNT from Mvr Drv"))
p.add_message(sequence.Uml_message (amu, mc, "Nothing to dismount"))
p.add_message(sequence.Uml_message (mc, amu, "put V in Mvr Drv"))
p.add_message(sequence.Uml_message (amu, arm, "(tape is transported to drive)"))
p.add_message(sequence.Uml_message (mc, mover, "OK"))

p.add_message(sequence.Uml_message (mover, td, "(FTT) online, then rewind"))
p.add_message(sequence.Uml_message (mover, td, "skip to flie, using the cookie"))

w0 = p.add_message(sequence.Uml_message (td, mover, "data"))
w1 = p.add_message(sequence.Uml_message (mover, encp, "read data"))


p.add_iteration(sequence.Uml_iteration(w0, w1))

p.add_message(sequence.Uml_message (td, mover, "READ ERROR"))

p.add_message(sequence.Uml_message (mover, vc, "V is system:readonly stats"))
p.add_message(sequence.Uml_message (mover, encp, "READ_ERROR, retry"))
p.add_message(sequence.Uml_message (encp, lm,   "please retry my read"))

p.add_message(sequence.Uml_message (mover, td, "mt offline"))
p.add_message(sequence.Uml_message (mover, mc, "put volume away"))
p.add_message(sequence.Uml_message (mc, amu, "put volume away"))
p.add_message(sequence.Uml_message (amu, arm, "volume is transported to shelf"))
p.add_message(sequence.Uml_message (amu, mc, "OK"))
p.add_message(sequence.Uml_message (mover, lm, "unilateral unbind V"))
p.add_message(sequence.Uml_message (mover, lm, "idle mover"))
p.set_geometry()
p.draw()


ps.put_trailer()
