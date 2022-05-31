#!/usr/bin/env Rscript

outf   <- fs::dir_ls(fs::path("data/pubmed.parquet/"))
numf   <- length(fs::dir_ls("download/ftp.ncbi.nlm.nih.gov/pubmed/baseline",regexp=".gz$"))
done   <- length(outf)
first  <- fs::file_info(outf) |> dplyr::pull(modification_time) |> min() |> lubridate::as_datetime()

total_secs  <- lubridate::interval(first,lubridate::now()) |> lubridate::as.difftime() |> lubridate::time_length() 
min_per_doc <- (total_secs/done) / 60
est_hour    <- (numf-done) * min_per_doc / 60

failures <- fs::dir_ls("log") |> length()

jsonlite::toJSON(list(
  elapsed = total_secs,
  doc_rem = numf - done,
  doc_don = done,
  failure = failures,
  doc_min = round(1/min_per_doc,2),
  est_hrs = round(est_hour,2)
),auto_unbox = T,pretty = T)