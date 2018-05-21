url <- "https://cn.dataone.org/cn/v2/resolve/https%3A%2F%2Fpasta.lternet.edu%2Fpackage%2Fdata%2Feml%2Fknb-lter-arc%2F20047%2F1%2Face205418a4a821b97bee2dbb69d7952"

url <- "https://cn.dataone.org/cn/v2/resolve/https%3A%2F%2Fpasta.lternet.edu%2Fpackage%2Fdata%2Feml%2Fknb-lter-gce%2F102%2F31%2F6c435602dd547d2aae19965c18c3f049"

getEMLData <- function(url) {
  
  ## Find id from url
  is_d1_cn_file <- grepl("https://cn.dataone.org/cn/v2/resolve/", url)
  if (!is_d1_cn_file) {
    stop('url is not a dataone resolvable url',
         'url should begin with "https://cn.dataone.org/cn/v2/resolve/"')
  }
  
  id <- gsub("https://cn.dataone.org/cn/v2/resolve/", "", url, fixed = TRUE)
  id <- URLdecode(id)
  
  ## Get DataObject
  cn <- dataone::CNode("PROD")
  nodes <- dataone::resolve(cn, id)
  d1c <- dataone::D1Client("PROD", nodes$data$nodeIdentifier[[1]])
  data_obj <- dataone::getDataObject(d1c, id)
  
  ## Get metadata files that document data
  metadata_id <- unlist(dataone::query(d1c@cn, list(q = paste0('identifier:"', id, '"'),
                                                    fl = "isDocumentedBy")))
  ## Gets non obsoleted metadata files
  metadata_id <- unlist(sapply(metadata_id, function(x) {
    unlist(dataone::query(d1c@cn, list(q = paste0('identifier:"', x, '" AND -obsoletedBy:*'),
                                       fl = "identifier")))
  }))
  
  if (length(metadata_id) > 1) {
    stop("Mulitiple metadata files found")  ## TODO: Handle multiple metadata files more cleanly
  }
  
  metadata <- rawToChar(dataone::getObject(d1c@mn, metadata_id))
  
  ## Read eml metadata
  eml <- EML::read_eml(metadata)
  
  ## Get data object's metadata
  i_obj <- datamgmt::which_in_eml(eml@dataset@dataTable, "url", function(x) {any(grepl(id, x))})
  obj_metadata <- eml@dataset@dataTable[[i_obj]]
  
  if (length(i_obj) == 0) {
    i_obj <- datamgmt::which_in_eml(eml@dataset@otherEntity, "url", function(x) {any(grepl(id, x))})
    obj_metadata <- eml@dataset@otherEntity[[i_obj]]
    
  } else if (length(i_obj) == 0) {
    obj_metadata <- NULL
    warning("No object metadata found") ##TODO: Handle object metadata search better
  }
  
  ## Get attributes for data_obj
  attributes <- EML::get_attributes(obj_metadata@attributeList)$attributes
  
  ## Get data for data_obj
  data <- data_obj@data
  data <- suppressWarnings(suppressMessages(readr::read_delim(file = data,
                                                              delim = ",",
                                                              col_names = FALSE)))
  ## Set column names
  ### Could use obj_metadata@physical[[1]]@dataFormat@textFormat to help here but not uniformly used
  r <- 1
  is_colnames <- FALSE
  while (!is_colnames) {
    is_colnames <- all(attributes$attributeName == data[r, ])
    is_colnames <- ifelse(is.na(is_colnames), FALSE, is_colnames)
    r <- r + 1
    
    if (r > nrow(data)) {
      stop("Data columns don't match attribute column names") ## TODO: Be more flexible
    }
  }
  data <- data[r:nrow(data), ]
  
  parse_list <- apply(attributes, 1, function(x) {
    
    if (x["domain"] == "numericDomain") {
      out <- paste0('parse_number()')
      
    } else if (x["domain"] == "dateTimeDomain") {
      
      formatString <- x["formatString"]
      is_date <- grepl("-|\\/", formatString)
      is_time <- grepl(":", formatString)
      
      formatString <- gsub("YYYY|yyyy", "%Y", formatString)
      formatString <- gsub("DD|dd", "%d", formatString)
      formatString <- gsub("(MM|mm)(-|\\/)+", "%m\\2", formatString)
      formatString <- gsub("hh", "%H", formatString)
      formatString <- gsub("(:)+(MM|mm)", "\\1%M", formatString)
      
      if (is_date && is_time) {
        out <- paste0('parse_datetime(, format = "', formatString, '")')
        
      } else if (is_date) {
        out <- paste0('parse_date(, format = "', formatString, '")')
        
      } else if (is_time) {
        out <- paste0('parse_time(, format = "', formatString, '")')
      }
      
    } else {
      out <- "parse_character()"
    }
  })
  parse_data <- function(parse_list, data) {
    as.data.frame(lapply(seq_along(parse_list), function(i) {
      func <- gsub("(parse_.*\\()", paste0("\\1unlist(data[ ,", i,"])"), parse_list[i])
      eval(parse(text = func))
    }), stringsAsFactors = FALSE)
  }
  
  r <- 0
  has_warnings <- TRUE
  while (has_warnings) {
    r <- r + 1
    
    has_warnings <- tryCatch({
      parse_data(parse_list, data[r, ])
      FALSE
    }, warning = function(w) {
      TRUE
    })
  }
  data <- parse_data(parse_list, data[r:nrow(data), ])
  colnames(data) <- attributes$attributeName
  
  
  
  
  