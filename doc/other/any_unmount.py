import sequence
import ps

sequence.font = ps.CourierBold(8)

ps.put_header()

p = sequence.Uml_sequence_page("UNMOUNT ERROR IN this case rbot had error stowing the tape")

encp   = p.add_class_role(sequence.Uml_class_role("Encp"))
lm     = p.add_class_role(sequence.Uml_class_role("LibMan"))
mover  = p.add_class_role(sequence.Uml_class_role("Mover"))
mc     = p.add_class_role(sequence.Uml_class_role("MedChng"))
amu    = p.add_class_role(sequence.Uml_class_role("Amu"))
vc     = p.add_class_role(sequence.Uml_class_role("VClerk"))
#fc     = p.add_class_role(sequence.Uml_class_role("FClerk"))
ls     = p.add_class_role(sequence.Uml_class_role("logSvr"))
td     = p.add_class_role(sequence.Uml_class_role("TapeDrv"))
arm    = p.add_class_role(sequence.Uml_class_role("Arm"))
other_encps = p.add_class_role(sequence.Uml_class_role("Other Encps"))

p.add_message(sequence.Uml_message(lm, mover, "unbind"))
p.add_message(sequence.Uml_message(mover, td, "mt offline"))
# status for mt offline is not checked
p.add_message(sequence.Uml_message(mover, amu, "please move volu to shelf"))
p.add_message(sequence.Uml_message(amu, arm, "move the volume"))
p.add_message(sequence.Uml_message(arm, amu, "error"))
p.add_message(sequence.Uml_message(amu, mc, "error"))
p.add_message(sequence.Uml_message(mc, mover, "error"))
p.add_message(sequence.Uml_message(mover, ls, "I fear tape is hung"))
p.add_message(sequence.Uml_message(mover, vc, "Mark V is system: noaccess"))
p.add_message(sequence.Uml_message(mover, lm, "V is system: noaccess"))

regret = p.add_message(sequence.Uml_message (lm, other_encps, "regrets for reading volume V"))
p.add_iteration(sequence.Uml_iteration(regret, regret))

p.set_geometry()
p.draw()


ps.put_trailer()









