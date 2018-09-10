# ========== generating results =================

source("define_benchmark.r")

args = commandArgs(trailingOnly=TRUE)

if (args[1] == "small") {
	mode = "small"
	n1 = 10
	n2 = 10
} else if (args[1] == "medium") {
	mode = "medium"
	n1 = 10
	n2 = 20
} else {
	mode = "large"
	n1 = 20
	n2 = 20
}
N = 20

# lapply, type
mbm.spark.lapply.type <- run.mbm.spark.lapply.type(n2)
p <- mbm.spark.lapply.type %>% plot.box.mbm
filename <- sprintf("%slapply.type.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# lapply, len
mbm.spark.lapply.len <- run.mbm.spark.lapply.len(mode, n1)
p <- mbm.spark.lapply.len %>% plot.box.mbm
filename <- sprintf("%slapply.len.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# dapply, type
mbm.dapply.type <- run.mbm.dapply.type(n2)
p <- mbm.dapply.type %>% plot.box.mbm
filename <- sprintf("%sdapply.type.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# dapply, len
mbm.dapply.len <- run.mbm.dapply.len(mode, n1)
p <- mbm.dapply.len %>% plot.box.mbm
filename <- sprintf("%sdapply.len.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# dapply, ncol
mbm.dapply.ncol <- run.mbm.dapply.ncol(mode, n1)
p <- mbm.dapply.ncol %>% plot.box.mbm
filename <- sprintf("%sdapply.ncol.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# dapplyCollect, type
mbm.dapplyCollect.type <- run.mbm.dapplyCollect.type(N)
p <- mbm.dapplyCollect.type %>% plot.box.mbm
filename <- sprintf("%sdapplyCollect.type.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# dapplyCollect, len
mbm.dapplyCollect.len <- run.mbm.dapplyCollect.len(mode, N)
p <- mbm.dapplyCollect.len %>% plot.box.mbm
filename <- sprintf("%sdapplyCollect.len.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# dapplyCollect, ncol
mbm.dapplyCollect.ncol <- run.mbm.dapplyCollect.ncol(mode, n1)
p <- mbm.dapplyCollect.ncol %>% plot.box.mbm
filename <- sprintf("%sdapplyCollect.ncol.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# gapply, nkey
mbm.gapply.nkey <- run.mbm.gapply.nkey(mode, n1)
p <- mbm.gapply.nkey %>% plot.box.mbm
filename <- sprintf("%sgapply.nkey.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# gapply, nrow
mbm.gapply.nrow <- run.mbm.gapply.nrow(mode, n1)
p <- mbm.gapply.nrow %>% plot.box.mbm
filename <- sprintf("%sgapply.nrow.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# gapply, keytype
mbm.gapply.keytype <- run.mbm.gapply.keytype(n1)
p <- mbm.gapply.keytype %>% plot.box.mbm
filename <- sprintf("%sgapply.keytype.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# gapplyCollect, nkey
mbm.gapplyCollect.nkey <- run.mbm.gapplyCollect.nkey(mode, n1)
p <- mbm.gapplyCollect.nkey %>% plot.box.mbm
filename <- sprintf("%sgapplyCollect.nkey.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# gapplyCollect, nrow
mbm.gapplyCollect.nrow <- run.mbm.gapplyCollect.nrow(mode, n1)
p <- mbm.gapplyCollect.nrow %>% plot.box.mbm
filename <- sprintf("%sgapplyCollect.nrow.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


# gapplyCollect, keytype
mbm.gapplyCollect.keytype <- run.mbm.gapplyCollect.keytype(n1)
p <- mbm.gapplyCollect.keytype %>% plot.box.mbm
filename <- sprintf("%sgapplyCollect.keytype.%s.png", dir_path, mode)
ggsave(filename, width=7, height=4)


tmp <- rbind(
	mbm.spark.lapply.type,
	mbm.spark.lapply.len,
	mbm.dapply.type,
	mbm.dapply.len,
	mbm.dapply.ncol,
	mbm.dapplyCollect.type,
	mbm.dapplyCollect.len,
	mbm.dapplyCollect.ncol,
	mbm.gapply.nkey,
	mbm.gapply.nrow,
	mbm.gapply.keytype,
	mbm.gapplyCollect.nkey, 
	mbm.gapplyCollect.nrow,
	mbm.gapplyCollect.keytype)

# compute throughput
tmp_size <- merge(tmp, df.sizes, by.x = "expr", by.y = "obj_names", all.x=TRUE)
tmp_size$throughput <- round(tmp_size$obj_sizes*1000000/tmp_size$time, digits=2)  # bytes per second

# plot throughput
p <- tmp_size %>% plot.box.throughput
filename <- sprintf("%sall.throughput.%s.png", dir_path, mode)
ggsave(filename, width=7, height=6)

# save raw data to csv file
towrite <- tmp_size[order(tmp_size$expr, tmp_size$time),]
write.csv(towrite, file="results/results.csv", row.names = F)

# save mean value in ml.perf_metrics format
# timestamp: timestamp, benchmarkId: string, benchmarkName: string,
# metricName: string, metricValue: string, isLargerBetter: boolean, parameters map<string, string>
op <- options(digits.secs = 3)
curTimestamp <- Sys.time()
benchmarkName <- "com.databricks.spark.sql.perf.sparkr.UserDefinedFunction"
metricName <- "throughput.byte.per.second"
isLargerBetter <- TRUE

perf_metric <- aggregate(towrite$throughput, list(towrite$expr), mean)
names(perf_metric) <- c("benchmarkId", "throughput")
perf_metric$timestamp <- curTimestamp
perf_metric$benchmarkName <- benchmarkName
perf_metric$metricName <- metricName
perf_metric$isLargerBetter <- isLargerBetter
perf_metric$parameters <- NULL
write.csv(perf_metric, file="results/perf_metrics.csv", row.names = F)
