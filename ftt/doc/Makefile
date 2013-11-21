
user_guide.ps: user_guide_nodsc.ps 
	ps2ps -dLanguageLevel=1 user_guide_nodsc.ps user_guide.ps

user_guide_nodsc.ps : user_guide.html
	html2ps -R -n user_guide.html > user_guide_nodsc.ps
