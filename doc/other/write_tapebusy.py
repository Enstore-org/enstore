import sequence
import ps

ps.put_header()

p = sequence.Uml_sequence_page("WRITE_TAPEBUSY The robot thinks the tape is in some other drive")

encp   = p.add_class_role(sequence.Uml_class_role("Encp"))
lm     = p.add_class_role(sequence.Uml_class_role("LibMan"))
mover  = p.add_class_role(sequence.Uml_class_role("Mover"))
mc     = p.add_class_role(sequence.Uml_class_role("MedChng"))
amu    = p.add_class_role(sequence.Uml_class_role("Amu"))
vc     = p.add_class_role(sequence.Uml_class_role("VOlCLerk"))
ls     = p.add_class_role(sequence.Uml_class_role("logSvr"))
other_encp = p.add_class_role(sequence.Uml_class_role("other Encps"))

p.add_message(sequence.Uml_message(mover,lm,"you summoned me"))
p.add_message(sequence.Uml_message(lm, mover,"put F on V"))
p.add_message(sequence.Uml_message(mover, encp,"Hello"))
p.add_message(sequence.Uml_message (encp, mover, "port= ip="))
p.add_message(sequence.Uml_message (mover, mc, "loadvol V"))
p.add_message(sequence.Uml_message (mc, amu, "DISMOUNT from Mvr Drv"))
p.add_message(sequence.Uml_message (amu, mc, "Mvr Drv has no volume"))
p.add_message(sequence.Uml_message (mc, amu, "put V in Mvr Drv"))
p.add_message(sequence.Uml_message (amu, mc, "V is in other drv"))
p.add_message(sequence.Uml_message (mc, mover, "TAPEBUSY"))
p.add_message(sequence.Uml_message (mover, vc, "V is system:no access"))
p.add_message(sequence.Uml_message (mover, ls, "WRITE_TAPEBUSY, vol is XYZ"))
p.add_message(sequence.Uml_message (mover, encp, "WRITE_TAPEBUSY, please retry"))
p.add_message(sequence.Uml_message (mover, lm, "unilateral unbind WRITE_TAPEBUSY"))
p.add_message(sequence.Uml_message (mover, lm, "idle mover"))

p.set_geometry()
p.draw()


ps.put_trailer()
