set output 'bytes.pts.ps'
set terminal postscript color
set title 'Total Bytes Transferred Per Day'
set xlabel 'Date'
set timefmt "%Y-%m-%d"
set xdata time
set xrange [ : ]
set ylabel 'Bytes'
set grid
set yrange [0: ]
set format x "%m-%d"
set key top
plot 'bytes.pts' using 1:2 t '' w boxes


