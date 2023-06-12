set datafile separator ','
set ylabel 'Latency (μs, 95 %ile)'
set xlabel 'Target QPS'

set terminal png size 2000,1000 enhanced font "sans serif,20"
set output "plot.png"

set key outside

plot '../results/results_0.csv' using 1:9 with linespoints lw 3 pt 6 ps 4 title "0 Indices", \
     '../results/results_1.csv' using 1:5 with linespoints lw 3 pt 5 ps 4 title "1 Index", \
     '../results/results_5.csv' using 1:5 with linespoints lw 3 pt 5 ps 4 title "5 Indices", \
     '../results/results_10.csv' using 1:5 with linespoints lw 3 pt 5 ps 4 title "10 Indices", \
     '../results/results_50.csv' using 1:5 with linespoints lw 3 pt 5 ps 4 title "50 Indices"
