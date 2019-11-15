library(dplyr)

args <- commandArgs(trailingOnly = TRUE)
t <- data.frame()
for (arg in args) {
	a <- strsplit(sub(".log", "", basename(arg)), "\\.")
	a <- a[[1]]
	server = a[[1]]
	articles = as.numeric(sub("a$", "", a[[2]]))
	target = as.numeric(sub("t$", "", a[[3]]))
	read_perc = as.numeric(sub("r$", "", a[[4]]))
	skew = a[[5]]

	con <- file(arg, open = "r")
	cpu = list()
	actual = 0
	clients = 0
	in_results = TRUE
	ts <- data.frame()
	while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
		if (grepl("load average", line)) {
			l <- strsplit(line, " ")
			l <- l[[1]]
			cpu = append(cpu, list(as.numeric(sub(",", "", l[[12]]))))

		} else if (startsWith(line, "# ")) {
			if (startsWith(line, "# actual ops")) {
				actual = actual + as.numeric(sub("# actual ops/s: ", "", line))
				clients = clients + 1
				in_results = TRUE
			} else if (startsWith(line, "# server stats")) {
				in_results = FALSE
			}
		} else if (in_results) {
			v <- read.table(text = line)
			this <- data.frame(op=v[,1], pct=as.factor(v[,2]), sjrn=as.numeric(v[,3]), rmt=as.numeric(v[,4]))
			ts <- rbind(ts, this)
		}
	} 

	ts$sjrn[ts$sjrn > 10000000] = Inf
	ts$rmt[ts$rmt > 10000000] = Inf
	ts <- ts %>% group_by(op, pct) %>% summarize(sjrn_max = max(sjrn),
						     sjrn_min = min(sjrn),
						     sjrn = median(sjrn),
						     rmt_max = max(rmt),
						     rmt_min = min(rmt),
						     rmt = median(rmt))

	ts$server = server
	ts$articles = articles
	ts$target = target
	ts$actual = actual
	ts$readp = read_perc
	ts$skew = skew
	cpu <- unlist(cpu)
	ts$cpu = median(cpu)
	ts$cpu_min = min(cpu)
	ts$cpu_max = max(cpu)

	# expected batch size when batch time 1ms
	# ops/s = (target / clients)
	# s/batch = 0.001
	# ops/batch = target * 0.001 / clients
	# let's then compute the processing time normalized to per op
	# rmt = s/batch, want s/op, so /= ops/batch => *= batch/ops
	ts$rmt_norm = (ts$rmt * clients * 1000.0) / target

	ts <- as.data.frame(ts)

	t <- rbind(t, ts)
	close(con)
}
#t
t = t[t$pct != 100,]
t = t[t$pct != 99,]
t$server <- as.factor(t$server)
t$actual <- t$actual/1000000.0
t$target <- t$target/1000000.0
t$rmt <- t$rmt/1000.0
t$rmt_min <- t$rmt_min/1000.0
t$rmt_max <- t$rmt_max/1000.0
t$sjrn <- t$sjrn/1000.0
t$sjrn_min <- t$sjrn_min/1000.0
t$sjrn_max <- t$sjrn_max/1000.0
mx_rmt = max(t[t$pct == 50,]$rmt)
mx_rmt_norm = max(t[t$pct == 50,]$rmt_norm)
t$sjrn <- pmin(t$sjrn, 10*mx_rmt) # otherwise ggplot tries to plot all the way to 100k
t$sjrn_min <- pmin(t$sjrn_min, 10*mx_rmt)
t$sjrn_max <- pmin(t$sjrn_max, 10*mx_rmt)
cap <- 10
#t

library(ggplot2)
p <- ggplot(data=t, aes(x=target, y=rmt, color=server, linetype=pct, shape=server, ymin=rmt_min, ymax=rmt_max))
p <- p + coord_trans(x = "identity", y = "identity", limy=c(0, min(cap, mx_rmt)))
p <- p + facet_grid(skew ~ op)
p <- p + scale_linetype_manual(breaks=c(50,95,99), values=c("solid","dotted","dashed"))
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_errorbar(width = 0.3) + geom_line()
p <- p + xlab("target load (Mops/s)") + ylab("batch processing time (ms)") + ggtitle("Batch processing time")
ggsave('plot-batch.png',plot=p,width=10,height=4)

p <- ggplot(data=t, aes(x=target, y=rmt_norm, color=server, linetype=pct, shape=server))
#p <- p + coord_trans(x = "identity", y = "log10", limy=c(1, mx_rmt_norm))
p <- p + scale_y_log10()
p <- p + facet_grid(skew ~ op)
p <- p + scale_linetype_manual(breaks=c(50,95,99), values=c("solid","dotted","dashed"))
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_line()
p <- p + xlab("target load (Mops/s)") + ylab("op processing time (µs)") + ggtitle("Estimated per-operation latency")
ggsave('plot-op-norm.png',plot=p,width=10,height=4)

# for sojourn time, read and write should basically be the same
worst <- data.frame(t %>% group_by(target, pct, server, skew) %>% summarize(sjrn = max(sjrn), sjrn_min = max(sjrn_min), sjrn_max = max(sjrn_max)))
p <- ggplot(data=worst, aes(x=target, y=sjrn, color=server, linetype=pct, shape=server, ymin=sjrn_min, ymax=sjrn_max))
p <- p + coord_trans(x = "identity", y = "identity", limy=c(0, min(cap, 1.2*mx_rmt)))
p <- p + facet_wrap(~ skew)
p <- p + scale_linetype_manual(breaks=c(50,95,99), values=c("solid","dotted","dashed"))
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_errorbar(width = 0.3) + geom_line()
p <- p + xlab("target load (Mops/s)") + ylab("sojourn time (ms)") + ggtitle("Sojourn time")
ggsave('plot-sjrn.png',plot=p,width=10,height=4)

t <- t[t$pct == 50,]
t <- t[t$op == "read",]
p <- ggplot(data=t, aes(x=target, y=cpu, color=server, shape=server, ymin=cpu_min, ymax=cpu_max))
p <- p + facet_wrap(~ skew)
p <- p + geom_hline(yintercept=16)
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_errorbar(width = 0.1) + geom_line()
p <- p + xlab("target load (Mops/s)") + ylab("cpu load") + ggtitle("Server CPU load")
ggsave('plot-cpu.png',plot=p,width=10,height=4)

#library(plotly)
#p <- subplot(p, p, nrows = 2, margin = 0.1)
#htmlwidgets::saveWidget(p, "plots.html", selfcontained=F)
#browseURL("plots.html")
