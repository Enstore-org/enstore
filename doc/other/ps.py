import point

""" 
	Embarrasingly rudimentary postscript module
	
	The module gives you 72 pitch drawing surface on an 8 1/2 x 11 
	portrait type paper.  It sets the ULC as x=0m y=0

	it gives text, line and arrow drawing primitives

	It gives a Courier font class with font metrics.

"""


color_mods = {
	'red'   : " 1.0 0.0 0.0 setrgbcolor ",
	'green' : " 0.0 1.0 0.0 setrgbcolor ",
	'blue'  : " 0.0 0.0 1.0 setrgbcolor ",
	'black' : ""
}

style_mods = {
	'solid' : "",
        'dash'  : " [2] 0 setdash "
}

width_mods = {
	1 : " 1 setlinewidth ", 
	2 : " 2 setlinewidth ",
	3 : " 3 setlinewidth ",
	4 : " 4 setlinewidth "
}

class G_mods :
	def __init__(self, color="black", style="solid", width=1):
		self.color_mod = color_mods[color]
		self.style_mod = style_mods[style]
		self.width_mod = width_mods[width]
	def mods(self):
		return self.color_mod + self.style_mod + self.width_mod

def_mods = G_mods()


header_text = """
%!PS-Adobe-3.0
%%Creator:pretty-poor-uml drawer
%%Orientation: Portrait
%%Pages: 1
%%DocumentFonts: (atend)
%%EndComments
%%BeginProlog

%%EndProlog
%%Page: 1 1

% move origin to upper left corner
72 0 mul 72 11.00 mul translate
1.0 dup neg scale
"""
def put_header() :
	print header_text

text_template = """
%% text
0 setgray
/%s findfont [%d 0 0 -%d 0 0] makefont setfont
   gsave
      %d %d moveto (%s) show
   grestore
"""

def put_text(text, p, size=12, font_name="Courier") :
	scale = int((17 * size)/12 + 0.99)
	scale = size
	print text_template % (font_name, scale, scale, p.x(), p.y(), text)


line_template = """
%% line
0 setgray
gsave
   newpath
      %s
      %d %d  moveto
      %d %d  lineto
   stroke
grestore
"""

def put_line(p0, p1, mods=def_mods) :
	print line_template % (mods.mods(), p0.x(), p0.y(), p1.x(), p1.y())

arrow_template = """
%% arrow
gsave
  %s
  %d %d moveto
  %f rotate
  %d 0 rlineto
  currentpoint
  160 rotate
  20 0 rlineto
  -160 rotate
  moveto
  -160 rotate
  20 0 rlineto
  160 rotate
  stroke
grestore
"""
def put_arrow(p0, p1, mods=def_mods) :
	rot = point.degrees(p0, p1)
	length = point.distance(p0, p1)
	print arrow_template % (mods.mods(), p0.x(), p0.y(), rot, length)

trailer_template = """
showpage

%%Trailer
%%DocumentFonts: Courier-Bold
%%EOF
"""

def put_trailer () :
	print trailer_template

class Courier :
	def __init__(self, size) :
		self.size = size
		self.font_name="Courier"
	def height(self):
		return point.Point(0, int (self.size * 12.0 / 17.0))
	def length(self, string):
		width = self.size*(10.0/17.0)
		width = width * len(string)
		width = width + 0.99
		width = int(width)
		return point.Point(width, 0)
	def bb_size(self, string) :
		return self.length(string) + self.height()
	def draw(self, string, p) :
		put_text(string, p, self.size, self.font_name)

class CourierBold(Courier) :
	def __init__(self, size) :
		Courier.__init__(self, size)
		self.font_name = "Courier-Bold"

class CourierItalic(Courier) :
	def __init__(self, size) :
		Courier.__init__(self, size)
		self.font_name = "Courier-Italic"

class CourierBoldItalic(Courier) :
	def __init__(self, size) :
		Courier.__init__(self, size)
		self.font_name = "Courier-BoldItalic"

if __name__ == "__main__" :

	norm = Courier(36)
	bold = CourierBold(36)
	ital = CourierItalic(36)
	boit = CourierBoldItalic(36)
	put_header()
	norm.draw("normal weight 36 p.t.", point.Point(100,100))
	bold.draw("bold weight 36 p.t.", point.Point(100,150))
	ital.draw("normal weight, italic 36 p.t.", point.Point(100,200))
	boit.draw("bold italic, 36 p.t.", point.Point(100,250))

	put_line (point.Point(100,300), point.Point(200, 300))
	put_line (point.Point(200,300), point.Point(300, 300),
		G_mods(width=2))
	put_line (point.Point(300,300), point.Point(400, 300),
		G_mods(width=3))
	put_line (point.Point(400,300), point.Point(500, 300),
		G_mods(width=4))

	put_line (point.Point(100,350), point.Point(200, 350))
	put_line (point.Point(200,350), point.Point(300, 350),
		G_mods(color='red'))
	put_line (point.Point(300,350), point.Point(400, 350),
		G_mods(color='green'))
	put_line (point.Point(400,350), point.Point(500, 350),
		G_mods(color='blue'))

	put_line (point.Point(100,400), point.Point(200, 400))
	put_line (point.Point(200,400), point.Point(300, 400),
		G_mods(style='solid'))
	put_line (point.Point(300,300), point.Point(400, 300),
		G_mods(style='dash'))

	put_arrow (point.Point(100,500), point.Point(200, 500))
	put_arrow (point.Point(100,600), point.Point(200, 600),
				G_mods(color="red", width=4))
	put_trailer()





