from ps import *
import point


font = Courier(12*2)
horizontal_border =   point.Point(3,0)
vertical_border =     point.Point(0,2)
vertical_text_space = point.Point(0,2)
horizontal_text_indent = point.Point (5,0)
		
class Uml_class :

	def __init__(self) :
		self.name_len_max = point.Point(0,0)
		self.n_text_ele = 0
		self.method_list = []
		self.member_list = []
		self.ulc = point.Point (0,0)
		self.class_name =  ""

	def add_text_ele(self, text) :
		if font.length(text).x() > self.name_len_max.x() :
			self.name_len_max =  font.length(text)
		self.n_text_ele = self.n_text_ele + 1

	def add_class_name(self, class_name):
		self.class_name =  class_name
		self.add_text_ele(class_name)


	def add_method(self, method_name):
		self.method_list.append(method_name+"\(\)")
		self.add_text_ele(method_name + "()")

	def add_member(self, member_name):
		self.member_list.append(member_name)
		self.add_text_ele(member_name)


	def set_geometry(self, ulc):
		self.ulc = ulc
		box_width = self.name_len_max  + \
				horizontal_border*2
		box_height = font.height()*self.n_text_ele + \
				vertical_text_space*(self.n_text_ele*2) + \
				vertical_border*6
		self.lrc_offset = box_width + box_height
	
	def top_join_point(self) :
		half = self.lrc_offset.x()
		half = int(half/2)
		return self.ulc + point.Point(half, 0)

	def bottom_join_point(self) :
		half = self.lrc_offset.x()
		half = int(half/2)
		return self.ulc + point.Point(half, self.lrc_offset.y())


	def draw(self):
		self.vcursor = self.ulc
		self.draw_box()
		self.draw_class_name()
		self.draw_seperator()
		self.draw_members()
		self.draw_seperator()
		self.draw_methods()

	def draw_box(self) : 
		#  c0 c1
		#  c2 c3
		c0 = self.vcursor
		c1 = self.vcursor + self.lrc_offset.project_x()
		c2 = self.vcursor + self.lrc_offset.project_y()
		c3 = self.vcursor + self.lrc_offset
		put_line(c0, c1)
		put_line(c0, c2)
		put_line(c2, c3)
		put_line(c3, c1)
		self.vcursor = self.vcursor +  vertical_border

	def draw_class_name(self) : 
		self.put_text(self.class_name)

	def draw_seperator(self) : 
		self.vcursor = self.vcursor +  vertical_border
		put_line(self.vcursor, 
			 self.vcursor + self.lrc_offset.project_x() )
		self.vcursor = self.vcursor +  vertical_border

	def draw_members(self) : 
		for m in self.member_list :
			self.put_text(m)

	def draw_methods(self) : 
		for m in self.method_list :
			self.put_text(m)

	def put_text(self, text) :
		self.vcursor = self.vcursor + vertical_text_space
		self.vcursor = self.vcursor + font.height()
		font.draw (text, self.vcursor + horizontal_border)
		self.vcursor = self.vcursor + vertical_text_space

def join_class (northern, southern) :
	put_line(northern.bottom_join_point(), southern.top_join_point())

def test() :

	put_header()

	b = Uml_class()
	b.add_class_name("bdb.Bdb")
	b.add_method("- trace_dispatch")
	b.add_method("+ set_trace")
	b.add_method("- user_line")
	b.add_method("- user_call")
	b.add_method("- user_return")
	b.add_method("- user_exception")
	b.set_geometry(point.Point(100,100))
	b.draw()

	p = Uml_class()
	p.add_class_name("pdb.Pdb")
	p.add_method("- user_line")
	p.add_method("- user_call")
	p.add_method("- user_return")
	p.add_method("- user_exception")
	p.add_method("- do_break")
	p.add_method("- do_where")
	p.add_method("- do_up")
	p.add_method("- do_down")
	p.add_method("- do_step")
	p.add_method("- do_continue")
	p.add_method("- do_quit")
	p.set_geometry(point.Point(100,350))
	p.draw()

	c = Uml_class()
	c.add_class_name("cmd.Cmd")
	c.add_method("+ cmdloop")
	c.add_method("+ onecmd")
	c.add_method("- print_topics")
	c.set_geometry(point.Point(500,190))
	c.draw()


	h = Uml_class()
	h.add_class_name("tdb.Tdb")
	h.add_method("- trace_dispatch")
	h.add_method("+ set_trace")
	h.set_geometry(point.Point(101,800))
	h.draw()

	join_class(b,p)
	join_class(c,p)
	join_class(p,h)
	
	t = Uml_class()
	t.add_class_name("threading.Thread")
	t.add_method("+ start")
	t.add_method("+ run")
	t.add_method("+ ...")
	t.set_geometry(point.Point(500,800))
	t.draw()


	tl = Uml_class()
	tl.add_class_name("tdb.TdbListener")
	tl.add_member("+ host")
	tl.add_member("+ port")
	tl.add_method("+ run")
	tl.set_geometry(point.Point(300,1000))
	tl.draw()


	tm = Uml_class()
	tm.add_class_name("tdb.TdbMonitor")
	tm.add_member("+ iniFile")
	tm.add_member("+ outFile")
	tm.add_method("+ run")
	tm.add_method("- cmd_help")
	tm.add_method("- line_cmd_eval")
	tm.add_method("- line_cmd_exec")
	tm.add_method("- cmd_pdb")
	tm.add_method("- cmd_....")
	tm.set_geometry(point.Point(700,1000))
	tm.draw()
	
	hi = Uml_class()
	hi.add_class_name("tdb.Hackio")
	hi.add_member("+ outFile")
	hi.add_method("+ write")
	hi.set_geometry(point.Point(100,1200))
	hi.draw()

	join_class (t, tl)
	join_class (t, tm)
	
	put_trailer()
 
if __name__ == "__main__" :

	test()













