set output 'mlat.pts.ps'
set terminal postscript color
set title 'Mount Latency in Seconds'
set xlabel 'Date'
set timefmt "%Y-%m-%d:%H:%M:%S"
set yrange [0 : ]
set xdata time
set xrange [ : ]
set ylabel 'Latency'
set grid
set format x "%m-%d"
plot 'mlat.pts' using 1:2 t '' with points
