sync <- read.table(text=grep("GET|PUT", readLines("sync.log"), value=T))
sync[,4] = "sync"
sswap <- read.table(text=grep("GET|PUT", readLines("sync-swap.log"), value=T))
sswap[,4] = "sync+swap"
park <- read.table(text=grep("GET|PUT", readLines("parking_lot.log"), value=T))
park[,4] = "park"
pswap <- read.table(text=grep("GET|PUT", readLines("parking_lot-swap.log"), value=T))
pswap[,4] = "park+swap"
t <- rbind(sync, sswap, park, pswap)
t <- subset(t, grepl("GET|PUT", t[,2]))

t[,2] = sub("[0-9]?:", "", t[,2])
t <- data.frame(at=as.numeric(t[,1])/1000000000.0, method=t[,2], opss=as.numeric(t[,3]), variant=t[,4])
t

library(ggplot2)
p <- ggplot(data=t, aes(x=at, y=opss, color=variant))
p <- p + expand_limits(y=0)
p <- p + geom_point(size = 0.3, alpha = 0.5) + geom_smooth()
p <- p + xlab("time") + ylab("ops/s") + ggtitle("ops/s")
p <- p + facet_wrap(~method, ncol=1, scales="free")
ggsave('plot.png',plot=p,width=8,height=6)
