set output 'bpt.pts.log.ps'
set terminal postscript color
set title 'Individual Transfer Activity'
set xlabel 'Date'
set timefmt "%Y-%m-%d:%H:%M:%S"
set xdata time
set xrange [ : ]
set ylabel 'Bytes per Transfer'
set grid
set format x "%y-%m-%d"
set logscale y
plot 'bpt.pts' using 1:2 t '' with points
set output 'bpt.pts.ps'
set pointsize 2
set nologscale y
set yrange [0: ]
plot 'bpt.pts' using 1:2 t '' w impulses, 'bytes.pts' using 1:5 t 'mean file size' w points
