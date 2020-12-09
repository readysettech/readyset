args <- commandArgs(trailingOnly = TRUE)
t <- data.frame()
for (arg in args) {
	a <- strsplit(sub(".log", "", arg), "-")
	a <- a[[1]]
	shards = as.numeric(a[[2]])
	workers = as.numeric(a[[3]])
	rthreads = as.numeric(a[[4]])
	target = as.numeric(a[[5]])

	con <- file(arg, open = "r")
	actual = target
	while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
		if (startsWith(line, "# ")) {
			if (startsWith(line, "# actual ops")) {
				actual <- as.numeric(sub("# actual ops/s: ", "", line))
			}
		} else {
			v <- read.table(text = line)
			t <- rbind(t, data.frame(op=v[,1], pct=as.factor(v[,2]), sjrn=as.numeric(v[,3]), rmt=as.numeric(v[,4]), shards=shards, workers=workers, rthreads=rthreads, target=target, actual=actual))
		}
	} 
	close(con)
}
t = t[t$pct != 100,]
t = t[t$workers == 1,]
t$actual <- t$actual/1000000.0
t$target <- t$target/1000000.0
#t

#t$batchsize <- factor(t$batchsize, levels = t$batchsize[order(t$batchsize)])
#t$max <- factor(t$max, levels = t$max[order(t$max)])
#
t$sjrn <- pmin(t$sjrn, 500) # otherwise ggplot tries to plot all the way to 100k

library(ggplot2)

p <- ggplot(data=t, aes(x=actual, y=rmt, color=pct, linetype=op, shape=op))
p <- p + coord_trans(x = "identity", y = "identity", limy=c(0, 150))
p <- p + facet_grid(shards ~ rthreads, labeller = label_both)
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_line()
p <- p + xlab("offered load (Mops/s)") + ylab("batch processing time (µs)")
ggsave('plot-batch.png',plot=p,width=10,height=4)

p <- ggplot(data=t, aes(x=actual, y=sjrn, color=pct, linetype=op, shape=op))
p <- p + coord_trans(x = "identity", y = "identity", limy=c(0, 150))
p <- p + facet_grid(shards ~ rthreads, labeller = label_both)
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_line()
p <- p + xlab("offered load (Mops/s)") + ylab("sojourn time (µs)")
ggsave('plot-sjrn.png',plot=p,width=10,height=4)
