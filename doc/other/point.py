import math
"""
   point.py

  
   Point -- class representing a grid of points wth positive integer coordiates
	Point(x,y)
	integer p.x(), p.y()
	Point p.project_x()m p.project_y()
	Point = Point + Point
	Point = Point - Point
	Point = Point * Integer

"""
class Point :
	def __init__(self, x, y) :
		self._x = x
		self._y = y
		self._assert()
		
	def _assert(self) :
		if type(self._x) is not type(1) : raise "Impossible x"
		if type(self._y) is not type(1) : raise "Impossible y"
		if self._x < 0 : raise "Impossible x"
		if self._y < 0 : raise "Impossible y"
	def x(self) :
		self._assert()
		return self._x
	def y(self) :
		self._assert()
		return self._y
	def project_x(self) :
		self._assert()
		return Point(self._x, 0)
	def project_y(self) :
		self._assert()
		return Point (0,self._y)
	def __add__(self, p) :
		self._assert()
		return Point(self._x + p.x(), self._y + p.y())
	def __sub__(self, p) :
		self._assert()
		return Point(self._x - p.x(), self._y - p.y())
	def __mul__(self, num) :
		self._assert()
		return Point(int(self._x * num), int(self._y *num))

def leftmost(a, b) :
	if a.x() < b.x() : 
		return a
	else:
		return b
def distance(p0, p1) :
	return  math.sqrt( (p0.x()-p1.x())**2 + (p0.y()-p1.y())**2 )

def degrees(p0, p1) :
	return math.atan2(p1.y() - p0.y(), p1.x() - p0.x()) \
				/ math.pi * 180

if __name__ ==  "__main__" :
	def chk_isPoint(p) :
		if type(p) is not type(Point(0,0)) : 
			raise "type failure"
	def chk_is0(p) :
		if p.x() is not 0 or p.y() is not 0 : 
			raise "compare failure"
 	def chk_isInt(i) :
		if type(i) is not type(1):
			raise "type failure"

	chk_isInt(Point(0,1).x())
	chk_isInt(Point(0,1).y())
	chk_isPoint(Point(10,10).project_x())
	chk_isPoint(Point(10,10).project_y())
	chk_is0(Point(100,100).project_x().project_y())
	chk_isPoint(Point(1,2) + Point(2,3))
	chk_isPoint(Point(0,0)*3)
	chk_is0 (Point(4,5) - Point(1,2) - Point(1,1)*3)
	chk_is0(Point(1,3) + Point(2,5) - Point(3,8))
	chk_is0(leftmost(Point(0,0), Point(10, 10)))

