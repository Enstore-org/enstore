import sequence
import ps

sequence.font = ps.Courier(12)

ps.put_header()

p = sequence.Uml_sequence_page("NORMAL SUCCESSFUL WRITE")

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


p.add_message(sequence.Uml_message(mover,lm,"you summoned me"))
p.add_message(sequence.Uml_message(lm, mover,"put F on V"))
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
p.add_message(sequence.Uml_message (mover, vc, "V is system:writing"))
p.add_message(sequence.Uml_message (mover, td, "skip to eod, using the cookie"))

w0 = p.add_message(sequence.Uml_message (encp, mover, "data"))
w1 = p.add_message(sequence.Uml_message (mover, td, "data"))
p.add_iteration(sequence.Uml_iteration(w0, w1))
p.add_message(sequence.Uml_message (mover, td, "WRITE EOF"))

p.add_message(sequence.Uml_message (mover, vc, "V is o.k, new EOD cookie, stats"))
p.add_message(sequence.Uml_message (mover, fc, "New file"))
p.add_message(sequence.Uml_message (fc, mover, "O.K, here is BFID"))

p.add_message(sequence.Uml_message (mover, encp, "I'm done, here is BFID + info"))
p.add_message(sequence.Uml_message (mover, lm, "have bound volume V"))

p.set_geometry()
p.draw()


ps.put_trailer()
