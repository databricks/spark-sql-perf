library("SparkR")
library("microbenchmark")
library("magrittr")
library("ggplot2")
library("pryr")

sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))

dir_path <- "results/"

# For benchmarking spark.lapply, we generate

#     lists 

# 1. with different types (7):
#     Length = 100,
#     Type = integer, 
#            logical, 
#            double, 
#            character1, 
#            character10, 
#            character100, 
#            character1k. 

data.list.type.double <- runif(100)
data.list.type.int <- as.integer(data.list.type.double * 100)
data.list.type.logical <- data.list.type.double < 0.5
data.list.type.char1 <- sapply(1:100, function(x) { "a" })
data.list.type.char10 <- sapply(1:100, function(x) { "0123456789" })
data.list.type.char100 <- sapply(1:100, function(x) { paste(replicate(20, "hello"), collapse = "") })
data.list.type.char1k <- sapply(1:100, function(x) { paste(replicate(200, "hello"), collapse = "") })

# 2. with different lengths (4):
#     Type = integer,
#     Length = 10,
#              100,
#              1k,
#              10k

data.list.len.10 <- 1:10
data.list.len.100 <- 1:100
data.list.len.1k <- 1:1000
data.list.len.10k <- 1:10000

# For benchmarking dapply+rnow/dapplyCollect, we generate

#     data.frame

# 1. with different types (7):
#     nrow = 100,
#     ncol = 1,
#     Type = integer, 
#            logical, 
#            double, 
#            character1, 
#            character10, 
#            character100, 
#            character1k.

data.df.type.double <- data.frame(data.list.type.double) %>% createDataFrame %>% cache
data.df.type.int <- data.frame(data.list.type.int) %>% createDataFrame %>% cache
data.df.type.logical <- data.frame(data.list.type.logical) %>% createDataFrame %>% cache
data.df.type.char1 <- data.frame(data.list.type.char1) %>% createDataFrame %>% cache
data.df.type.char10 <- data.frame(data.list.type.char10) %>% createDataFrame %>% cache
data.df.type.char100 <- data.frame(data.list.type.char100) %>% createDataFrame %>% cache
data.df.type.char1k <- data.frame(data.list.type.char1k) %>% createDataFrame %>% cache

# counting to materialize the cache
data.df.type.double %>% nrow
data.df.type.int %>% nrow
data.df.type.logical %>% nrow
data.df.type.char1 %>% nrow
data.df.type.char10 %>% nrow
data.df.type.char100 %>% nrow
data.df.type.char1k %>% nrow

# 2. with different lengths (4):
#     ncol = 1,
#     Type = integer,
#     nrow = 10,
#            100,
#            1k,
#            10k

data.df.len.10 <- data.frame(data.list.len.10) %>% createDataFrame %>% cache
data.rdf.len.100 <- data.frame(data.list.len.100)
data.df.len.100 <- data.rdf.len.100 %>% createDataFrame %>% cache
data.df.len.1k <- data.frame(data.list.len.1k) %>% createDataFrame %>% cache
data.df.len.10k <- data.frame(data.list.len.10k) %>% createDataFrame %>% cache

# counting to materialize the cache
data.df.len.10 %>% nrow
data.df.len.100 %>% nrow
data.df.len.1k %>% nrow
data.df.len.10k %>% nrow

# 3. with different ncols (3):
#     nrow = 100,
#     Type = integer,
#     ncol = 1,
#            10,
#            100

data.df.ncol.1 <- data.df.len.100
data.df.ncol.10 <- data.frame(rep(data.rdf.len.100, each = 10)) %>% createDataFrame %>% cache
data.df.ncol.100 <- data.frame(rep(data.rdf.len.100, each = 100)) %>% createDataFrame %>% cache

# counting to materialize the cache
data.df.ncol.1 %>% nrow
data.df.ncol.10 %>% nrow
data.df.ncol.100 %>% nrow

# For benchmarking gapply+rnow/gapplyCollect, we generate

#     data.frame

# 1. with different number of keys (3):
#     ncol = 2,
#     nrow = 1k,
#     Type = <integer, double>,
#     nkeys = 10,
#             100,
#             1000

data.rand.1k <- runif(1000)

data.df.nkey.10 <- data.frame(key = rep(1:10, 100), val = data.rand.1k) %>% createDataFrame %>% cache
data.df.nkey.100 <- data.frame(key = rep(1:100, 10), val = data.rand.1k) %>% createDataFrame %>% cache
data.df.nkey.1k <- data.frame(key = 1:1000, val = data.rand.1k) %>% createDataFrame %>% cache

# counting to materialize the cache
data.df.nkey.10 %>% nrow
data.df.nkey.100 %>% nrow
data.df.nkey.1k %>% nrow

# 2. with different lengths (4):
#     ncol = 2,
#     Type = <integer, double>,
#     nkeys = 10,
#     nrow = 10,
#            100,
#            1k,
#            10k

data.df.nrow.10 <- data.frame(key = 1:10, val = runif(10)) %>% createDataFrame %>% cache
data.df.nrow.100 <- data.frame(key = rep(1:10, 10), val = runif(100)) %>% createDataFrame %>% cache
data.df.nrow.1k <- data.frame(key = rep(1:10, 100), val = data.rand.1k) %>% createDataFrame %>% cache
data.df.nrow.10k <- data.frame(key = rep(1:10, 1000), val = runif(10000)) %>% createDataFrame %>% cache

# counting to materialize the cache
data.df.nrow.10 %>% nrow
data.df.nrow.100 %>% nrow
data.df.nrow.1k %>% nrow
data.df.nrow.10k %>% nrow

# 3. with different key types (3):
#     ncol = 2,
#     nkeys = 10,
#     nrow = 1k,
#     Type = <integer, double>
#            <character10, double>
#            <character100, double>

key.char10 <- sapply(1:10, function(x) { sprintf("%010d", x) })
key.char100 <- sapply(1:10, function(x) { sprintf("%0100d", x) })

data.df.keytype.int <- data.df.nrow.1k
data.df.keytype.char10 <- data.frame(key = rep(key.char10, 100), val = data.rand.1k) %>% createDataFrame %>% cache
data.df.keytype.char100 <- data.frame(key = rep(key.char100, 10), val = data.rand.1k) %>% createDataFrame %>% cache

# counting to materialize the cache
data.df.keytype.int %>% nrow
data.df.keytype.char10 %>% nrow
data.df.keytype.char100 %>% nrow

# ========== benchmark functions ==============

# plot utils
plot.box.mbm <- function(mbm_result) {
	ggplot(mbm_result, aes(x = expr, y = time/1000000)) + 
	geom_boxplot(outlier.colour = "red", outlier.shape = 1) + labs(x = "", y = "Time (milliseconds)") +
	geom_jitter(position=position_jitter(0.2), alpha = 0.2) + 
	coord_flip()
}
plot.box.throughput <- function(throughput_result) {
	ggplot(throughput_result, aes(x = expr, y = throughput)) + 
	geom_boxplot(outlier.colour = "red", outlier.shape = 1) + labs(x = "", y = "Throughput (B/s)") +
	geom_jitter(position=position_jitter(0.2), alpha = 0.2) + 
	coord_flip()
}

# dummy function
func.dummy <- function(x) { x }

# group function 
gfunc.mean <- function(key, sdf) {
	y <- data.frame(key, mean(sdf$val), stringsAsFactors = F)
	names(y) <- c("key", "val")
	y
}

# lapply, type
run.mbm.spark.lapply.type <- function(n) {
	microbenchmark(
		"lapply.int.100" = { spark.lapply(data.list.type.int, func.dummy) },
		"lapply.double.100" = { spark.lapply(data.list.type.double, func.dummy) },
		"lapply.logical.100" = { spark.lapply(data.list.type.logical, func.dummy) },
		"lapply.char1.100" = { spark.lapply(data.list.type.char1, func.dummy) },
		"lapply.char10.100" = { spark.lapply(data.list.type.char10, func.dummy) },
		"lapply.char100.100" = { spark.lapply(data.list.type.char100, func.dummy) },
		"lapply.char1k.100" = { spark.lapply(data.list.type.char1k, func.dummy) },
		times = n
	)
}

# lapply, len
run.mbm.spark.lapply.len <- function(mode, n) {
	if (mode == "small") {
		microbenchmark(
			"lapply.len.10" = { spark.lapply(data.list.len.10, func.dummy) },
			"lapply.len.100" = { spark.lapply(data.list.len.100, func.dummy) },
			times = n
		)
	} else if (mode == "medium") {
		microbenchmark(
			"lapply.len.10" = { spark.lapply(data.list.len.10, func.dummy) },
			"lapply.len.100" = { spark.lapply(data.list.len.100, func.dummy) },
			"lapply.len.1k" = { spark.lapply(data.list.len.1k, func.dummy) },
			times = n
		)
	} else {
		microbenchmark(
			"lapply.len.10" = { spark.lapply(data.list.len.10, func.dummy) },
			"lapply.len.100" = { spark.lapply(data.list.len.100, func.dummy) },
			"lapply.len.1k" = { spark.lapply(data.list.len.1k, func.dummy) },
			"lapply.len.10k" = { spark.lapply(data.list.len.10k, func.dummy) },
			times = n
		)
	}
}

# dapply, type
run.mbm.dapply.type <- function(n) {
	microbenchmark(
		"dapply.int.100" = { dapply(data.df.type.int, func.dummy, schema(data.df.type.int)) %>% nrow },
		"dapply.double.100" = { dapply(data.df.type.double, func.dummy, schema(data.df.type.double)) %>% nrow },
		"dapply.logical.100" = { dapply(data.df.type.logical, func.dummy, schema(data.df.type.logical)) %>% nrow },
		"dapply.char1.100" = { dapply(data.df.type.char1, func.dummy, schema(data.df.type.char1)) %>% nrow },
		"dapply.char10.100" = { dapply(data.df.type.char10, func.dummy, schema(data.df.type.char10)) %>% nrow },
		"dapply.char100.100" = { dapply(data.df.type.char100, func.dummy, schema(data.df.type.char100)) %>% nrow },
		"dapply.char1k.100" = { dapply(data.df.type.char1k, func.dummy, schema(data.df.type.char1k)) %>% nrow },
		times = n
	)
}

# dapply, len
run.mbm.dapply.len <- function(mode, n) {
	if (mode == "small") {
		microbenchmark(
			"dapply.len.10" = { dapply(data.df.len.10, func.dummy, schema(data.df.len.10)) %>% nrow },
			"dapply.len.100" = { dapply(data.df.len.100, func.dummy, schema(data.df.len.100)) %>% nrow },
			times = n
		)
	} else if (mode == "medium") {
		microbenchmark(
			"dapply.len.10" = { dapply(data.df.len.10, func.dummy, schema(data.df.len.10)) %>% nrow },
			"dapply.len.100" = { dapply(data.df.len.100, func.dummy, schema(data.df.len.100)) %>% nrow },
			"dapply.len.1k" = { dapply(data.df.len.1k, func.dummy, schema(data.df.len.1k)) %>% nrow },
			times = n
		)
	} else {
		microbenchmark(
			"dapply.len.10" = { dapply(data.df.len.10, func.dummy, schema(data.df.len.10)) %>% nrow },
			"dapply.len.100" = { dapply(data.df.len.100, func.dummy, schema(data.df.len.100)) %>% nrow },
			"dapply.len.1k" = { dapply(data.df.len.1k, func.dummy, schema(data.df.len.1k)) %>% nrow },
			"dapply.len.10k" = { dapply(data.df.len.10k, func.dummy, schema(data.df.len.10k)) %>% nrow },
			times = n
		)
	}
}

# dapply, ncol
run.mbm.dapply.ncol <- function(mode, n) {
	if (mode == "small") {
		microbenchmark(
			"dapply.ncol.1" = { dapply(data.df.ncol.1, func.dummy, schema(data.df.ncol.1)) %>% nrow },
			"dapply.ncol.10" = { dapply(data.df.ncol.10, func.dummy, schema(data.df.ncol.10)) %>% nrow },
			times = n
		)
	} else {
		microbenchmark(
			"dapply.ncol.1" = { dapply(data.df.ncol.1, func.dummy, schema(data.df.ncol.1)) %>% nrow },
			"dapply.ncol.10" = { dapply(data.df.ncol.10, func.dummy, schema(data.df.ncol.10)) %>% nrow },
			"dapply.ncol.100" = { dapply(data.df.ncol.100, func.dummy, schema(data.df.ncol.100)) %>% nrow },
			times = n
		)
	}

}

# dapplyCollect, type
run.mbm.dapplyCollect.type <- function(n) {
	microbenchmark(
		"dapplyCollect.int.100" = { dapplyCollect(data.df.type.int, func.dummy) },
		"dapplyCollect.double.100" = { dapplyCollect(data.df.type.double, func.dummy) },
		"dapplyCollect.logical.100" = { dapplyCollect(data.df.type.logical, func.dummy) },
		"dapplyCollect.char1.100" = { dapplyCollect(data.df.type.char1, func.dummy) },
		"dapplyCollect.char10.100" = { dapplyCollect(data.df.type.char10, func.dummy) },
		"dapplyCollect.char100.100" = { dapplyCollect(data.df.type.char100, func.dummy) },
		"dapplyCollect.char1k.100" = { dapplyCollect(data.df.type.char1k, func.dummy) },
		times = n
	)
}

# dapplyCollect, len
run.mbm.dapplyCollect.len <- function(mode, n) {
	if (mode != "large") {
		microbenchmark(
			"dapplyCollect.len.10" = { dapplyCollect(data.df.len.10, func.dummy) },
			"dapplyCollect.len.100" = { dapplyCollect(data.df.len.100, func.dummy) },
			"dapplyCollect.len.1k" = { dapplyCollect(data.df.len.1k, func.dummy) },
			times = n
		)
	} else {
		microbenchmark(
			"dapplyCollect.len.10" = { dapplyCollect(data.df.len.10, func.dummy) },
			"dapplyCollect.len.100" = { dapplyCollect(data.df.len.100, func.dummy) },
			"dapplyCollect.len.1k" = { dapplyCollect(data.df.len.1k, func.dummy) },
			"dapplyCollect.len.10k" = { dapplyCollect(data.df.len.10k, func.dummy) },
			times = n
		)
	}
}

# dapplyCollect, ncol
run.mbm.dapplyCollect.ncol <- function(mode, n) {
	if (mode == "small") {
		microbenchmark(
			"dapplyCollect.ncol.1" = { dapplyCollect(data.df.ncol.1, func.dummy) },
			"dapplyCollect.ncol.10" = { dapplyCollect(data.df.ncol.10, func.dummy) },
			times = n
		)
	} else {
		microbenchmark(
			"dapplyCollect.ncol.1" = { dapplyCollect(data.df.ncol.1, func.dummy) },
			"dapplyCollect.ncol.10" = { dapplyCollect(data.df.ncol.10, func.dummy) },
			"dapplyCollect.ncol.100" = { dapplyCollect(data.df.ncol.100, func.dummy) },
			times = n
		)
	}
}

# gapply, nkey
run.mbm.gapply.nkey <- function(mode, n) {
	if (mode == "small") {
		microbenchmark(
			"gapply.nkey.10" = { gapply(data.df.nkey.10, data.df.nkey.10$key, gfunc.mean, schema(data.df.nkey.10)) %>% nrow },
			"gapply.nkey.100" = { gapply(data.df.nkey.100, data.df.nkey.100$key, gfunc.mean, schema(data.df.nkey.100)) %>% nrow },
			times = n
		)
	} else if (mode == "medium") {
		microbenchmark(
			"gapply.nkey.10" = { gapply(data.df.nkey.10, data.df.nkey.10$key, gfunc.mean, schema(data.df.nkey.10)) %>% nrow },
			"gapply.nkey.100" = { gapply(data.df.nkey.100, data.df.nkey.100$key, gfunc.mean, schema(data.df.nkey.100)) %>% nrow },
			"gapply.nkey.1k" = { gapply(data.df.nkey.1k, data.df.nkey.1k$key, gfunc.mean, schema(data.df.nkey.1k)) %>% nrow },
			times = n
		)
	} else {
		microbenchmark(
			"gapply.nkey.10" = { gapply(data.df.nkey.10, data.df.nkey.10$key, gfunc.mean, schema(data.df.nkey.10)) %>% nrow },
			"gapply.nkey.100" = { gapply(data.df.nkey.100, data.df.nkey.100$key, gfunc.mean, schema(data.df.nkey.100)) %>% nrow },
			"gapply.nkey.1k" = { gapply(data.df.nkey.1k, data.df.nkey.1k$key, gfunc.mean, schema(data.df.nkey.1k)) %>% nrow },
			times = n
		)
	}
}

# gapply, nrow
run.mbm.gapply.nrow <- function(mode, n) {
	if (mode == "small") {
		microbenchmark(
			"gapply.nrow.10" = { gapply(data.df.nrow.10, data.df.nrow.10$key, gfunc.mean, schema(data.df.nrow.10)) %>% nrow },
			"gapply.nrow.100" = { gapply(data.df.nrow.100, data.df.nrow.100$key, gfunc.mean, schema(data.df.nrow.100)) %>% nrow },
			times = n
		)
	} else if (mode == "medium") {
		microbenchmark(
			"gapply.nrow.10" = { gapply(data.df.nrow.10, data.df.nrow.10$key, gfunc.mean, schema(data.df.nrow.10)) %>% nrow },
			"gapply.nrow.100" = { gapply(data.df.nrow.100, data.df.nrow.100$key, gfunc.mean, schema(data.df.nrow.100)) %>% nrow },
			"gapply.nrow.1k" = { gapply(data.df.nrow.1k, data.df.nrow.1k$key, gfunc.mean, schema(data.df.nrow.1k)) %>% nrow },
			times = n
		)
	} else {
		microbenchmark(
			"gapply.nrow.10" = { gapply(data.df.nrow.10, data.df.nrow.10$key, gfunc.mean, schema(data.df.nrow.10)) %>% nrow },
			"gapply.nrow.100" = { gapply(data.df.nrow.100, data.df.nrow.100$key, gfunc.mean, schema(data.df.nrow.100)) %>% nrow },
			"gapply.nrow.1k" = { gapply(data.df.nrow.1k, data.df.nrow.1k$key, gfunc.mean, schema(data.df.nrow.1k)) %>% nrow },
			"gapply.nrow.10k" = { gapply(data.df.nrow.10k, data.df.nrow.10k$key, gfunc.mean, schema(data.df.nrow.10k)) %>% nrow },
			times = n
		)
	}
	
}

# gapply, keytype
run.mbm.gapply.keytype <- function(n) {
	microbenchmark(
		"gapply.keytype.int" = { gapply(data.df.keytype.int, data.df.keytype.int$key, gfunc.mean, schema(data.df.keytype.int)) %>% nrow },
		"gapply.keytype.char10" = { gapply(data.df.keytype.char10, data.df.keytype.char10$key, gfunc.mean, schema(data.df.keytype.char10)) %>% nrow },
		"gapply.keytype.char100" = { gapply(data.df.keytype.char100, data.df.keytype.char100$key, gfunc.mean, schema(data.df.keytype.char100)) %>% nrow },
		times = n
	)
}

# gapplyCollect, nkey
run.mbm.gapplyCollect.nkey <- function(mode, n) {
	if (mode == "small") {
		microbenchmark(
			"gapplyCollect.nkey.10" = { gapplyCollect(data.df.nkey.10, data.df.nkey.10$key, gfunc.mean) },
			"gapplyCollect.nkey.100" = { gapplyCollect(data.df.nkey.100, data.df.nkey.100$key, gfunc.mean) },
			times = n
		)
	} else if (mode == "medium") {
		microbenchmark(
			"gapplyCollect.nkey.10" = { gapplyCollect(data.df.nkey.10, data.df.nkey.10$key, gfunc.mean) },
			"gapplyCollect.nkey.100" = { gapplyCollect(data.df.nkey.100, data.df.nkey.100$key, gfunc.mean) },
			"gapplyCollect.nkey.1k" = { gapplyCollect(data.df.nkey.1k, data.df.nkey.1k$key, gfunc.mean) },
			times = n
		)
	} else {
		microbenchmark(
			"gapplyCollect.nkey.10" = { gapplyCollect(data.df.nkey.10, data.df.nkey.10$key, gfunc.mean) },
			"gapplyCollect.nkey.100" = { gapplyCollect(data.df.nkey.100, data.df.nkey.100$key, gfunc.mean) },
			"gapplyCollect.nkey.1k" = { gapplyCollect(data.df.nkey.1k, data.df.nkey.1k$key, gfunc.mean) },
			times = n
		)
	}
}

# gapplyCollect, nrow
run.mbm.gapplyCollect.nrow <- function(mode, n) {
	if (mode == "small") {
		microbenchmark(
			"gapplyCollect.nrow.10" = { gapplyCollect(data.df.nrow.10, data.df.nrow.10$key, gfunc.mean) },
			"gapplyCollect.nrow.100" = { gapplyCollect(data.df.nrow.100, data.df.nrow.100$key, gfunc.mean) },
			times = n
		)
	} else if (mode == "medium") {
		microbenchmark(
			"gapplyCollect.nrow.10" = { gapplyCollect(data.df.nrow.10, data.df.nrow.10$key, gfunc.mean) },
			"gapplyCollect.nrow.100" = { gapplyCollect(data.df.nrow.100, data.df.nrow.100$key, gfunc.mean) },
			"gapplyCollect.nrow.1k" = { gapplyCollect(data.df.nrow.1k, data.df.nrow.1k$key, gfunc.mean) },
			times = n
		)
	} else {
		microbenchmark(
			"gapplyCollect.nrow.10" = { gapplyCollect(data.df.nrow.10, data.df.nrow.10$key, gfunc.mean) },
			"gapplyCollect.nrow.100" = { gapplyCollect(data.df.nrow.100, data.df.nrow.100$key, gfunc.mean) },
			"gapplyCollect.nrow.1k" = { gapplyCollect(data.df.nrow.1k, data.df.nrow.1k$key, gfunc.mean) },
			"gapplyCollect.nrow.10k" = { gapplyCollect(data.df.nrow.10k, data.df.nrow.10k$key, gfunc.mean) },
			times = n
		)
	}
}

# gapplyCollect, keytype
run.mbm.gapplyCollect.keytype <- function(n) {
	microbenchmark(
		"gapplyCollect.keytype.int" = { gapplyCollect(data.df.keytype.int, data.df.keytype.int$key, gfunc.mean) },
		"gapplyCollect.keytype.char10" = { gapplyCollect(data.df.keytype.char10, data.df.keytype.char10$key, gfunc.mean) },
		"gapplyCollect.keytype.char100" = { gapplyCollect(data.df.keytype.char100, data.df.keytype.char100$key, gfunc.mean) },
		times = n
	)
}

# ============== compute object sizes ==================
ser <- function(data) {
	serialize(data, connection = NULL)
}
get.sizes <- function(obj_names, objs) {
	obj_sizes <- objs %>% lapply(ser) %>% lapply(object.size)
	data.frame(cbind(obj_names, obj_sizes))
}

# lapply
sizes.lapply.type <- get.sizes(
	list(
		"lapply.double.100",
		"lapply.int.100",
		"lapply.logical.100",
		"lapply.char1.100",
		"lapply.char10.100", 
		"lapply.char100.100",
		"lapply.char1k.100"),
	list(
		data.list.type.double,
		data.list.type.int,
		data.list.type.logical,
		data.list.type.char1,
		data.list.type.char10,
		data.list.type.char100,
		data.list.type.char1k)
)
sizes.lapply.len <- get.sizes(
	list(
		"lapply.len.10",
		"lapply.len.100",
		"lapply.len.1k",
		"lapply.len.10k"),
	list(
		data.list.len.10,
		data.list.len.100,
		data.list.len.1k,
		data.list.len.10k)
)

# data for dapply
df.dapply.type <- lapply(list(
	data.list.type.double,
	data.list.type.int,
	data.list.type.logical,
	data.list.type.char1,
	data.list.type.char10,
	data.list.type.char100,
	data.list.type.char1k), data.frame)
df.dapply.len <- lapply(list(
		data.list.len.10,
		data.list.len.100,
		data.list.len.1k,
		data.list.len.10k), data.frame)
df.dapply.ncol <- lapply(list(
		data.list.len.100,
		rep(data.rdf.len.100, each = 10),
		rep(data.rdf.len.100, each = 100)), data.frame)
 
# dapply
sizes.dapply.type <- get.sizes(
	list(
		"dapply.double.100",
		"dapply.int.100",
		"dapply.logical.100",
		"dapply.char1.100",
		"dapply.char10.100", 
		"dapply.char100.100",
		"dapply.char1k.100"),
	df.dapply.type
)
sizes.dapply.len <- get.sizes(
	list(
		"dapply.len.10",
		"dapply.len.100",
		"dapply.len.1k",
		"dapply.len.10k"),
	df.dapply.len
)
sizes.dapply.ncol <- get.sizes(
	list(
		"dapply.ncol.1",
		"dapply.ncol.10",
		"dapply.ncol.100"),
	df.dapply.ncol
)

# dapplyCollect
sizes.dapplyCollect.type <- get.sizes(
	list(
		"dapplyCollect.double.100",
		"dapplyCollect.int.100",
		"dapplyCollect.logical.100",
		"dapplyCollect.char1.100",
		"dapplyCollect.char10.100", 
		"dapplyCollect.char100.100",
		"dapplyCollect.char1k.100"),
	df.dapply.type
)
sizes.dapplyCollect.len <- get.sizes(
	list(
		"dapplyCollect.len.10",
		"dapplyCollect.len.100",
		"dapplyCollect.len.1k",
		"dapplyCollect.len.10k"),
	df.dapply.len
)
sizes.dapplyCollect.ncol <- get.sizes(
	list(
		"dapplyCollect.ncol.1",
		"dapplyCollect.ncol.10",
		"dapplyCollect.ncol.100"),
	df.dapply.ncol
)

# data for gapply
df.gapply.nkey <- list(
	data.frame(key = rep(1:10, 100), val = data.rand.1k),
	data.frame(key = rep(1:100, 10), val = data.rand.1k),
	data.frame(key = 1:1000, val = data.rand.1k))
df.gapply.nrow <- list(
	data.frame(key = 1:10, val = runif(10)), 
	data.frame(key = rep(1:10, 10), val = runif(100)),
	data.frame(key = rep(1:10, 100), val = data.rand.1k),
	data.frame(key = rep(1:10, 1000), val = runif(10000)))
df.gapply.keytype <- list(
	data.frame(key = rep(1:10, 100), val = data.rand.1k),
	data.frame(key = rep(key.char10, 100), val = data.rand.1k),
	data.frame(key = rep(key.char100, 10), val = data.rand.1k))

# gapply
sizes.gapply.nkey <- get.sizes(
	list(
		"gapply.nkey.10",
		"gapply.nkey.100",
		"gapply.nkey.1k"),
	df.gapply.nkey
)
sizes.gapply.nrow <- get.sizes(
	list(
		"gapply.nrow.10",
		"gapply.nrow.100",
		"gapply.nrow.1k",
		"gapply.nrow.10k"),
	df.gapply.nrow
)
sizes.gapply.keytype <- get.sizes(
	list(
		"gapply.keytype.int",
		"gapply.keytype.char10",
		"gapply.keytype.char100"),
	df.gapply.keytype
)

# gapplyCollect
sizes.gapplyCollect.nkey <- get.sizes(
	list(
		"gapplyCollect.nkey.10",
		"gapplyCollect.nkey.100",
		"gapplyCollect.nkey.1k"),
	df.gapply.nkey
)
sizes.gapplyCollect.nrow <- get.sizes(
	list(
		"gapplyCollect.nrow.10",
		"gapplyCollect.nrow.100",
		"gapplyCollect.nrow.1k",
		"gapplyCollect.nrow.10k"),
	df.gapply.nrow
)
sizes.gapplyCollect.keytype <- get.sizes(
	list(
		"gapplyCollect.keytype.int",
		"gapplyCollect.keytype.char10",
		"gapplyCollect.keytype.char100"),
	df.gapply.keytype
)

df.sizes <- rbind(
	sizes.lapply.type,
	sizes.lapply.len,
	sizes.dapply.type,
	sizes.dapply.len,
	sizes.dapply.ncol,
	sizes.dapplyCollect.type,
	sizes.dapplyCollect.len,
	sizes.dapplyCollect.ncol,
	sizes.gapply.nkey,
	sizes.gapply.nrow,
	sizes.gapply.keytype,
	sizes.gapplyCollect.nkey, 
	sizes.gapplyCollect.nrow,
	sizes.gapplyCollect.keytype)
df.sizes$obj_names <- unlist(df.sizes$obj_names)
df.sizes$obj_sizes <- unlist(df.sizes$obj_sizes)
