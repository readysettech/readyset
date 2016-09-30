args <- commandArgs(trailingOnly = TRUE)
t <- data.frame()
for (a in args) {
	a <- strsplit(a, ":")
	label = a[[1]][[1]]
	file = a[[1]][[2]]
	v <- read.table(textConnection(grep("percentile", readLines(file), value=TRUE)))
	t <- rbind(t, data.frame(t=v[,1], op=sub("0?:$", "", v[,2]), us=v[,3], perc=v[,4], variant=label, buffer=FALSE))
}
t$us[t$us == 0] <- 0.1

library(ggplot2)
p <- ggplot(data=t, aes(x=us, y=perc, color=variant))
p <- p + ylim(c(0,100))
p <- p + facet_grid(buffer ~ op)
p <- p + geom_line()
p <- p + scale_x_log10()
p <- p + xlab("latency (us)") + ylab("cdf") + ggtitle("Soup rwlock benchmark")
ggsave('latency-cdf.png',plot=p,width=10,height=10)
