set output 'rates.ps'
set terminal postscript color
set title 'Drive Transfer Rates'
set xlabel 'Date'
set timefmt "%m-%d-%Y-%H:%M:%S"
set yrange [0 : ]
set xdata time
set xrange [ : ]
set ylabel 'Tape R/W Rate'
set grid
set format x "%H:%M:%S\n%m/%d/%y"
plot 'dm05' using 2:3 t 'dm05' with steps,\
     'dm06' using 2:3 t 'dm06' with steps,\
     'dm07' using 2:3 t 'dm07' with steps,\
     'dm08' using 2:3 t 'dm08' with steps
