set output 'bpt.pts.ps'
set terminal postscript color
set title 'Individual Transfer Activity'
set xlabel 'Date'
set timefmt "%Y-%m-%d:%H:%M:%S"
set yrange [0 : ]
set xdata time
set xrange [ : ]
set ylabel 'Bytes per Transfer'
set grid
set format x "%y-%m-%d"
plot 'bpt.pts' using 1:2 t '' with impulses
