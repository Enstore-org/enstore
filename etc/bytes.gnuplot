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
set key top
plot 'bytes.pts' using 1:2 t '' w boxes, 'bytes.pts' using 1:3 t 'largest file size' w points, 'bytes.pts' using 1:4 t 'smallest file size' w points, 'bytes.pts' using 1:5 t 'mean file size' w points


