args <- commandArgs(trailingOnly = TRUE)
t <- data.frame()
for (a in args) {
	a <- strsplit(a, ":")
	label = a[[1]][[1]]
	file = a[[1]][[2]]
	max = as.numeric(sub("/.*", "", label))
	batchsize = as.numeric(sub(".*/", "", label))
	v <- read.table(text = grep("percentile|avg", grep("PUT|GET", readLines(file), value=TRUE), invert=TRUE, value=TRUE))
	t <- rbind(t, data.frame(t=v[,1], op=sub("\\d?\\+?:$", "", v[,2]), opss=v[,3], batchsize=batchsize, max=max))
}
t = t[t$op == "PUT",]
t$t <- t$t/1000000000.0
#t$opss <- t$opss/1000.0

t$batchsize <- factor(t$batchsize, levels = t$batchsize[order(t$batchsize)])
t$max <- factor(t$max, levels = t$max[order(t$max)])

library(ggplot2)
p <- ggplot(data=t, aes(x=t, y=opss, color=op))
p <- p + scale_y_log10(limits = c(1, NA))
p <- p + facet_grid(batchsize ~ max, labeller = label_both)
p <- p + geom_point(size = 0.1, alpha = 0.2)# + geom_smooth()
p <- p + xlab("time") + ylab("ops/s") + ggtitle("Soup migration timeline with varying batch/catch-up parameters")
ggsave('migration.png',plot=p,width=10,height=4)
