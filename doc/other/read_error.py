import sequence
import ps

sequence.font = ps.Courier(10)

ps.put_header()

p = sequence.Uml_sequence_page("READ_ERROR -- give up after 2 reads, many movers")

encp   = p.add_class_role(sequence.Uml_class_role("Encp"))
lm     = p.add_class_role(sequence.Uml_class_role("LibMan"))
m1      = p.add_class_role(sequence.Uml_class_role("Mover 1"))
m2      = p.add_class_role(sequence.Uml_class_role("Mover 2"))
m3      = p.add_class_role(sequence.Uml_class_role("Mover 3"))
#mc     = p.add_class_role(sequence.Uml_class_role("MedChng"))
#amu    = p.add_class_role(sequence.Uml_class_role("Amu"))
vc     = p.add_class_role(sequence.Uml_class_role("VClerk"))
#fc     = p.add_class_role(sequence.Uml_class_role("FClerk"))
ls     = p.add_class_role(sequence.Uml_class_role("logSvr"))
#td     = p.add_class_role(sequence.Uml_class_role("TapeDrv"))
#arm    = p.add_class_role(sequence.Uml_class_role("Arm"))
oe    = p.add_class_role(sequence.Uml_class_role("other encps"))

p.add_message(sequence.Uml_message (encp, lm,   "please read F"))
p.add_message(sequence.Uml_message (lm, m1,   "summons"))
p.add_message(sequence.Uml_message(m1, lm,"you summoned me"))
p.add_message(sequence.Uml_message(lm, m1,"read F from V"))
p.add_message(sequence.Uml_message (m1, encp, "READ_ERROR"))
p.add_message(sequence.Uml_message (m1, lm, "unilateral unbind V status = read error"))
p.add_message(sequence.Uml_message (lm, m1, "unbind V"))
p.add_message(sequence.Uml_message (m1, lm, "idle mover"))

p.add_message(sequence.Uml_message (encp, lm,   "please read F"))
p.add_message(sequence.Uml_message (lm, m2,   "summons"))
p.add_message(sequence.Uml_message(m2,lm,"you summoned me"))
p.add_message(sequence.Uml_message(lm, m2,"read F from V"))
p.add_message(sequence.Uml_message (m2, encp, "READ_ERROR"))
p.add_message(sequence.Uml_message (m2, lm, "unilateral unbind V status = read error"))
p.add_message(sequence.Uml_message (lm, m2, "unbind V"))

p.add_message(sequence.Uml_message (encp, lm,   "please read F"))
p.add_message(sequence.Uml_message (lm, encp, "regrets"))
p.add_message(sequence.Uml_message (lm, vc, "volume is system: no access"))
p.add_message(sequence.Uml_message (lm, ls, "volume is marked no access"))

i = p.add_message(sequence.Uml_message (lm, oe, "regrets to other readers"))
p.add_iteration(sequence.Uml_iteration(i, i))
p.add_message(sequence.Uml_message (m2, lm, "idle mover"))


p.set_geometry()
p.draw()


ps.put_trailer()





