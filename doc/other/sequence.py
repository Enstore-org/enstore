import ps
import point

font = ps.CourierBold(12)
horizontal_border =   point.Point(3,0)
vertical_border =     point.Point(0,2)
vertical_text_space = point.Point(0,2)
horizontal_text_indent = point.Point (5,0)
page_ulc = point.Point(72,72)
page_bb_size = point.Point(int(6.5*72), 9*72) 

class Uml_class_role :

	def __init__(self, Name) :
	
		self.name = Name	
		self.urc = point.Point(0,0)
		self.bb_size  = font.height() + vertical_border*2 +\
			         font.length(Name) + horizontal_border*2

	def set_geometry(self, ulc):
		self.ulc = ulc
		self.llc = ulc + self.bb_size

	def bottom_join_point(self) :
		half = self.bb_size.x()
		half = int(half/2)
		return self.ulc + point.Point(half, self.bb_size.y())

	def get_bb_size(self):
		return self.bb_size

	def draw(self):
		self.draw_box()
		font.draw(self.name, 
			 self.ulc + vertical_border + 
			 horizontal_border + font.height())

	def draw_box(self) : 
		#  c0 c1
		#  c2 c3
		c0 = self.ulc
		c1 = self.ulc + self.bb_size.project_x()
		c2 = self.ulc + self.bb_size.project_y()
		c3 = self.ulc + self.bb_size
		ps.put_line(c0, c1)
		ps.put_line(c0, c2)
		ps.put_line(c2, c3)
		ps.put_line(c3, c1)

	def put_text(self, text) :
		where = self.ulc + vertical_border + horizontal_border
		font.draw (text, where)

class Uml_message:
	def __init__(self, frm, to, labeltext):
		if type(labeltext) is not type("") : raise "type error"
		self.frm = frm
		self.to = to
		self.text = labeltext
		self.bb_size = font.bb_size(labeltext) + vertical_border*2 \
				+ horizontal_border*2 		
		self.ulc = point.Point(0,0)

	def set_geometry(self, ulc) :
		f = point.Point(self.frm.bottom_join_point().x(), ulc.y())
		t = point.Point(self.to.bottom_join_point().x(),  ulc.y())
		self.ulc =  point.leftmost(f, t)
		self.lrc = self.ulc + self.bb_size
		self.arrow_frm = f + font.height() + vertical_border*2
		self.arrow_to = t + font.height() + vertical_border*2

	def draw(self) :
		where = self.ulc + horizontal_border + \
					font.height() + vertical_border
		font.draw (self.text, where)
		ps.put_arrow(self.arrow_frm, self.arrow_to)


class Uml_iteration :
	def __init__(self, mnorth, msouth) :
		self.mnorth = mnorth
		self.msouth = msouth
	
	def set_geometry(self):
		#  c0 c1
		#  c2 c3
		northy = self.mnorth.ulc.project_y() - vertical_border
		southy = self.msouth.lrc.project_y() + vertical_border
		leftx = page_ulc.project_x() + horizontal_border
		rghtx = (page_ulc + page_bb_size).project_x() - \
							horizontal_border
		self.c0 = northy + leftx
		self.c1 = northy + rghtx
		self.c2 = southy + leftx
		self.c3 = southy + rghtx

	def draw(self) :
		#  c0 c1
		#  c2 c3
		ps.put_line(self.c0, self.c1)
		ps.put_line(self.c0, self.c2)
		ps.put_line(self.c2, self.c3)
		ps.put_line(self.c3, self.c1)

class Uml_sequence_page: 
	def __init__(self, title):
		self.roles=[]
		self.messages=[]
		self.iterations=[]
		self.role_max_height = 0
		self.width = 0
		self.role_max_height = 0
		self.title = title

	def add_class_role(self, class_role) :
		self.roles.append(class_role)
		self.role_max_height = max (self.role_max_height, 
				class_role.get_bb_size().y())
	        self.width = self.width + \
				class_role.get_bb_size().x()
		return class_role #for cleaner notation in caller

	def add_message(self, message) :
		self.messages.append(message)
		return message   #for cleaner notation in caller

	def add_iteration(self, iteration) :
		self.iterations.append(iteration)

	def set_geometry(self) :
		hgap = (page_bb_size.x() - self.width) / len(self.roles)
		hgap = point.Point(int(hgap), 0)
		where = page_ulc
		where = where + font.height() + vertical_border*2 #title
		where = where + font.height() # gap between title and roles.
		for r in self.roles :
			r.set_geometry(where)
			where = where + \
				r.get_bb_size().project_x() + hgap
		text_stride = font.height()
		vhalfgap = page_bb_size.y() - \
				 text_stride.y()*len(self.messages)
  		vhalfgap = int(vhalfgap / len(self.messages) / 2)
		vhalfgap = min(vhalfgap, 20)  # small gap if few messages
		vhalfgap = point.Point(0, vhalfgap)
		vcursor = page_ulc.project_y() + \
				point.Point(0, self.role_max_height)
		for msg in self.messages :
			vcursor = vcursor + vhalfgap
			vcursor = vcursor + text_stride
			frm_pt = msg.frm.bottom_join_point().project_x()
			frm_pt = frm_pt + vcursor
			to_pt   = msg.to.bottom_join_point().project_x()
			to_pt   = to_pt + vcursor
			msg.set_geometry(point.leftmost(to_pt, frm_pt))
			vcursor = vcursor + vhalfgap
		for iteration in self.iterations :
			iteration.set_geometry()
		
	def draw(self) :
		font.draw(self.title, 
			page_ulc + font.height() + vertical_border)
		#draw roles
		for r in range(len(self.roles)) :
			self.roles[r].draw()
		#draw messages
		for m in range(len(self.messages)) :
			msg = self.messages[m]
			msg.draw()
		#draw lifelines
		for r in range(len(self.roles)) :
			frm = self.roles[r].bottom_join_point()
			to = frm + page_bb_size.project_y()
			ps.put_line(frm, to, ps.G_mods(style="dash"))
		#draw iterations
		for i in range(len(self.iterations)) :
			iteration = self.iterations[i]
			iteration.draw()
def test() :

	ps.put_header()

	e= Uml_class_role("Encp")
	m = Uml_class_role("Mover")
	l = Uml_class_role("libm")
	s = Uml_class_role("libshelf")
	d = Uml_class_role("tapedrive")
	
	p = Uml_sequence_page("title")
	p.add_class_role(e)
	p.add_class_role(m)
	p.add_class_role(l)
	p.add_class_role(s)
	p.add_class_role(d)

	p.add_message(Uml_message(m,l,"unbind"))
	m0 = p.add_message(Uml_message(m,l,"try"))
	m1 = p.add_message(Uml_message(l,m,"andtry"))
	p.add_message(Uml_message(l, m,"ok"))
	p.add_iteration(Uml_iteration(m0, m1))
	p.set_geometry()
	p.draw()

	
	ps.put_trailer()
 
if __name__ == "__main__" :

	test()
