library(fs)
library(purrr)
library(xml2,  include.only=c("read_xml","xml_children","as_list"))
library(XML,   include.only=c("parseDTD"))
library(arrow, include.only=c("write_parquet"))
library(furrr)

gz <- fs::dir_ls("download/ftp.ncbi.nlm.nih.gov/pubmed/baseline",regexp=".gz$")
el <- { # BUILD SCHEMA FOR XML ELEMENTS
  load.dtd <- \(path){ readLines(path) |> paste(col="\n") |> XML::parseDTD(asText=T,error=\(...){})}
  el    <- load.dtd("download/pubmed/pubmed_190101.dtd")$elements
  occur <- map(el, ~ pluck(.,"contents","ocur",.default=c(none=0)) |> names() |> tolower())
  child <- map(el, ~ pluck(.,"contents","elements") |> map("elements") |> keep(is.character))
  map2(occur,child,~ list(occur=.x,child=.y)) |> set_names(names(el))
}

process_file <- function(file,out,el){

  mkjson <- \(node){
    tag <- xml2::xml_name(node)
    purrr::when(el[[tag]],
      .$occur == "one or more"  ~ mapchild(node,setname=F),
       purrr::is_empty(.$child) ~ xml2::xml_text(node),
      !purrr::is_empty(.$child) ~ mapchild(node,setname=T),     
      T ~ { stop("illegal condition") })
  }

  mapchild <- function(node,setname){
    children = xml2::xml_children(node)
    purrr::map(children,mkjson) |> purrr::set_names(if(setname){ xml2::xml_name(children) }else{NULL})
  }

  articles   <- xml2::read_xml(file) |> xml2::xml_children()
  json       <- articles |> purrr::map_chr( ~ mkjson(.) |> jsonlite::toJSON(auto_unbox = T)) # HEAVY COMPUTE STEP
  pmid       <- articles |> xml2::xml_find_all("MedlineCitation/PMID") |> xml2::xml_text()
  tbl        <- tibble::tibble(pmid=pmid,json=json)
  tbl$source <- fs::path_file(file)
  
  arrow::write_parquet(tbl,out)
}

# OUTS =================================================================================
outfile  <- fs::dir_create("data/pubmed.parquet/") |>
  fs::path(fs::path_file(gz) |> fs::path_ext_remove() |> fs::path_ext_set("parquet")) |>
  fs::path_abs()

library(future)
plan(multisession)
furrr::future_walk2(gz, outfile, process_file, el=el)