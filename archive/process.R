# DEPENDENCIES =======================================================================================
library(fs)
library(purrr)
library(xml2, include.only=c("read_xml","xml_children","as_list"))
library(XML, include.only=c("parseDTD"))
library(arrow, include.only=c("write_parquet"))
library(future)
library(furrr)

el <- { # BUILD SCHEMA FOR XML ELEMENTS
  load.dtd <- \(path){ readLines(path) |> paste(col="\n") |> XML::parseDTD(asText=T,error=\(...){})}
  el    <- load.dtd("download/pubmed/pubmed_190101.dtd")$elements
  occur <- map(el, ~ pluck(.,"contents","ocur",.default=c(none=0)) |> names() |> tolower())
  child <- map(el, ~ pluck(.,"contents","elements") |> map("elements") |> keep(is.character))
  map2(occur,child,~ list(occur=.x,child=.y)) |> set_names(names(el))
}

# TRANSFORM =======================================================================================
process_file <- function(file,out,el){

  mapchild <- function(node,setname){
    children = xml2::xml_children(node)
    purrr::map(children,mkjson) |> purrr::set_names(if(setname){ xml2::xml_name(children) }else{NULL})
  }

  mkjson <- \(node){
    tag <- xml2::xml_name(node)
    purrr::when(el[[tag]],
      .$occur == "one or more"  ~ mapchild(node,setname=F),
       purrr::is_empty(.$child) ~ xml2::xml_text(node),
      !purrr::is_empty(.$child) ~ mapchild(node,setname=T),     
      T ~ { stop("illegal condition") })
  }

  art     <- xml2::read_xml(file) |> xml2::xml_children()
  json    <- art |> map(mkjson) |> map_chr(jsonlite::toJSON,auto_unbox = T) # COMPUTE STEP
  pmid    <- art |> xml2::xml_find_all("MedlineCitation/PMID") |> xml2::xml_text() |> as.integer()
  tbl     <- tibble::tibble(pmid=pmid,json=json)
  tbl$src <- fs::path_file(file)
  
  arrow::write_parquet(tbl,out)
}

# OUTS =================================================================================
gzfile  <- fs::dir_ls("download/ftp.ncbi.nlm.nih.gov/pubmed/baseline",regexp=".gz$")
outdir  <- fs::dir_create("brick/pubmed.parquet/") |> fs::path_abs()
outfile <- fs::path(outdir,fs::path_file(gzfile)) |> gsub(pat="xml.gz$",repl="parquet") 
io      <- list(i=gzfile,o=outfile) |> transpose() |> discard(~file.exists(.$o)) |> transpose()

# PROCESS WITH LOGGING
step <- \(i,o,el){
  res      <- safely(process_file)(i,o,el) 
  err_path <- fs::path("log",fs::path_file(i) |> fs::path_ext_set("error"))
  map_chr(res$error,jsonlite::toJSON,force=T) |> walk(write,file=errpath)
}

plan(multisession(workers = 15))
furrr::future_walk2(io$i, io$o, step, el=el)