set output 'bytes.pts.ps'
set terminal postscript color
set title 'Total Bytes Transferred Per Day'
set xlabel 'Date'
set timefmt "%Y-%m-%d"
set yrange [0 : ]
set xdata time
set xrange [ : ]
set ylabel 'Bytes'
set grid
set format x "%m-%d"
plot 'bytes.pts' using 1:2 t '' with boxes
