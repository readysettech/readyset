v = read.table(file("results.log"))
t <- data.frame(readers=v[,1], writers=v[,2], distribution=v[,3], variant=v[,4], opss=v[,5], op=v[,7])

library(plyr)
t$writers = as.factor(t$writers)
t$readers = as.numeric(t$readers)

# split the data into tx/readers and tx/refresh
compare_impl = t[grep("evmap-refresh", t$variant, invert = TRUE),]
compare_rate = t[grep("evmap-refresh", t$variant, invert = FALSE),]
compare_rate = rbind(compare_rate, compare_impl[compare_impl$variant == "evmap",])

r = compare_impl[compare_impl$op == "read",]
r <- ddply(r, c("readers", "writers", "distribution", "variant", "op"), summarise, opss = sum(opss))
w = compare_impl[compare_impl$op == "write",]
w <- ddply(w, c("readers", "writers", "distribution", "variant", "op"), summarise, opss = sum(opss))

library(ggplot2)


r$opss = r$opss / 1000000.0
p <- ggplot(data=r, aes(x=readers, y=opss, color=variant))
#p <- p + ylim(c(0, 2500))
p <- p + xlim(c(0, NA))
p <- p + facet_grid(distribution ~ writers, labeller = labeller(writers = label_both))
p <- p + geom_point(size = .4, alpha = .1)
p <- p + geom_line(size = .5)
#p <- p + stat_smooth(size = .5, se = FALSE)
p <- p + xlab("readers") + ylab("M reads/s") + ggtitle("Total reads/s with increasing # of readers")
ggsave('read-throughput.png',plot=p,width=10,height=6)


w$opss = w$opss / 1000000.0
p <- ggplot(data=w, aes(x=readers, y=opss, color=variant))
#p <- p + scale_y_log10(lim=c(1, NA))#5000))
p <- p + facet_grid(distribution ~ writers, labeller = labeller(writers = label_both))
p <- p + geom_point(size = 1, alpha = .2)
p <- p + geom_line(size = .5)
#p <- p + stat_smooth(size = .5, se = FALSE)
#p <- p + coord_cartesian(ylim=c(0,250))
p <- p + xlim(c(0, NA))
p <- p + xlab("readers") + ylab("M writes/s") + ggtitle("Total writes/s with increasing # of readers")
ggsave('write-throughput.png',plot=p,width=10,height=6)


library(scales)

w = compare_rate
w <- w[w$writers == 1,]
w <- w[w$readers == 1,]
w <- w[w$op == "write",]
w$variant = gsub("^evmap$", "evmap-refresh1", w$variant, perl = TRUE)
w$period = as.numeric(gsub("evmap-refresh([\\d]+)", "\\1", w$variant, perl = TRUE))
w$variant = gsub("evmap-refresh[\\d]+", "evmap", w$variant, perl = TRUE)
w$opss = w$opss / 1000000.0
p <- ggplot(data=w, aes(x=period, y=opss, color=distribution))
p <- p + geom_point(size = 1, alpha = .2)
p <- p + geom_line(size = .5)
p <- p + scale_x_continuous(trans="log2",
    breaks = trans_breaks("log2", function(x) 2^x),
    labels = trans_format("log2", math_format(2^.x)))
p <- p + xlab("refresh every N writes") + ylab("M writes/s") + ggtitle("Total writes/s with decreasing refresh frequency")
ggsave('write-with-refresh.png',plot=p,width=10,height=6)
