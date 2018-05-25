#' Downloads objects from DataOne Nodes to R Environment
#'
#' @param obj (character) An identifier or url for a DataONE object to download.
#' If the object is a metadata or resource map object, all data associated with that object will downloaded.
#' 
#' @importFrom dataone CNode query resolve D1Client getObject getDataObject
#' @importFrom EML read_eml eml_get get_attributes
#' @importFrom xml2 read_xml
#'
#' @return (D1Data) A list of data and metadata
#'
#' @examples
#' \dontrun{
#' obj <- "https://dx.doi.org/10.6073/pasta/edc06bbae6db80e801b6e52253f2ea09"
#' D1Data <- get_D1Data(obj)
#' 
#' obj <- "https://cn.dataone.org/cn/v2/resolve/urn%3Auuid%3A085ffc05-6e5d-4d5d-9b8c-6763e4bf85ac" #DATA
#' D1Data <- get_D1Data(obj)
#'}
#' @export
get_D1Data <- function(obj) {
  
  stopifnot(is.character(obj))
  obj_id <- utils::URLdecode(obj)
  
  ## Try to get DataONE obj_id from obj.
  ## Subquentially test if obj is an id after removing leading "text/" or "text="
  cn <- dataone::CNode("PROD")
  q_fmt <- '%s:"%s"'
  fl <- "identifier, formatType, obsoletedBy"
  while(nchar(obj_id) > 0) {
    results <- suppressMessages(
      dataone::query(cn,
                     list(q = sprintf(q_fmt, "identifier", obj_id),
                          fl = fl))
    )
    
    if (length(results) == 0) {
      obj_id <- gsub("^[^\\/=]+[\\/=]*", "", obj_id)
      
    } else {
      if (length(results) > 1) {
        stop("A unique DataOne ID could not be found for ", obj)
      }
      
      results <- unlist(results, recursive = FALSE)
      
      if (length(results$obsoletedBy) > 0) {
        warning(obj, "has been obsoletedBy ", results$obsoletedBy)
      }
      
      formatType <- results$formatType
      obj_id <- results$identifier
      break
    }
    
  }
  
  if (nchar(obj_id) == 0) {
    stop("The DataOne ID could not be found for ", obj)
  }
  
  ## Get resource, metadata, and data information for obj
  rm_id <- NULL
  meta_id <- NULL
  data_id <- NULL
  
  if (formatType == "RESOURCE") {
    rm_id <- obj_id
    results <- dataone::query(cn,
                              list(q = sprintf(q_fmt, "resourceMap", rm_id),
                                   fl = fl))
    
  } else if (formatType == "METADATA") {
    meta_id <- obj_id
    results <- dataone::query(cn,
                              list(q = sprintf(q_fmt, "isDocumentedBy", meta_id),
                                   fl = fl))
    
  } else if (formatType == "DATA") {
    data_id <- obj_id
    results <- dataone::query(cn,
                              list(q = sprintf(q_fmt, "documents", data_id),
                                   fl = fl))
    
  } else {
    stop(obj, "is not a data, metadata, or data package")
  }
  
  results <- unlist(lapply(results, function(x) {
    meta_id <- NULL
    data_id <- NULL
    if (length(x$obsoletedBy) == 0) {
      
      if (x$formatType == "METADATA") {
        meta_id = x$identifier
        
      } else if (x$formatType == "DATA") {
        data_id = x$identifier
      }
    }
    
    return(list(meta_id = meta_id, data_id = data_id))
  }), use.names = TRUE)
  meta_id <- unique(c(meta_id, results[names(results) == "meta_id"]))
  data_id <- unique(c(data_id, results[names(results) == "data_id"]))
  
  ## Set Node
  ## This assumes data and metadata are at the same node
  nodes <- dataone::resolve(cn, meta_id)
  d1c <- dataone::D1Client("PROD", nodes$data$nodeIdentifier[[1]])
  
  ## Download Metadata
  if (length(meta_id) == 0) {
    warning("No metadata records found")
    meta_obj <- NULL
    
  } else if (length(meta_id) > 1) {
    stop("Multiple metadata records found")
    
  } else {
    message("\nDownloading metadata ", meta_id, " ...")
    meta_obj <- dataone::getObject(d1c@mn, meta_id)
    message("Download complete")
  }
  
  ## Download Data
  data_obj <- lapply(data_id, function(x) {
    message("\nDownloading data ", x, " ...")
    data_obj_x <- dataone::getDataObject(d1c, x)
    message("Download complete")
    return(data_obj_x)
  })
  
  ## Read in Metadata
  if (is.null(meta_obj)) {
    pkg_metadata <- NULL
  } else {
    pkg_metadata <- rawToChar(meta_obj)
    pkg_metadata = tryCatch({
      EML::read_eml(pkg_metadata) # If eml make EML object 
      #TODO: update to eml2 when eml2 is stable
    }, error = function(e) {
      xml2::read_xml(pkg_metadata)  # If not make xml2 object
    })
  }
  
  ## Get data object metadata
  getObjMetadata <- function(id, entity) {
    urls <- EML::eml_get(entity, "url")
    if (any(grepl(id, urls))) {
      out <- entity
      
    } else {
      out <- NULL
    }
    
    return(out)
  }
  
  data_objects <- lapply(data_obj, function(x) {
    
    if (attr(class(pkg_metadata), "package") == "EML") {
      
      id <- x@sysmeta@identifier
      dataTable <- lapply(pkg_metadata@dataset@dataTable, function(y) getObjMetadata(id, y))
      spatialRaster <- lapply(pkg_metadata@dataset@spatialRaster, function(y) getObjMetadata(id, y))
      spatialVector <- lapply(pkg_metadata@dataset@spatialVector, function(y) getObjMetadata(id, y))
      storedProcedure <- lapply(pkg_metadata@dataset@storedProcedure, function(y) getObjMetadata(id, y))
      view <- lapply(pkg_metadata@dataset@view, function(y) getObjMetadata(id, y))
      otherEntity <- lapply(pkg_metadata@dataset@otherEntity, function(y) getObjMetadata(id, y))
      
      data_metadata <- c(dataTable, spatialRaster, spatialVector, storedProcedure, view, otherEntity)
      data_metadata <- data_metadata[!unlist(lapply(data_metadata, is.null))]
      
      if (length(data_metadata) == 0) {
        data_metadata <- NULL
        attribute_metadata <- NULL
        
      } else {
        
        if (length(data_metadata) > 1) {
        warning("Multiple metadata entries were found for ", id,".",
                "Only the first will be returned")
        }
        
        data_metadata <- data_metadata[[1]]
        attribute_metadata <- EML::get_attributes(data_metadata@attributeList)$attributes
      }
      
    } else {
      data_metadata <- NULL
      attribute_metadata <- NULL
    } 
    
    list(data = x@data,
         identifier = x@sysmeta@identifier,
         data_format = x@sysmeta@formatId,
         file_name = x@sysmeta@fileName,
         data_metadata = data_metadata,
         attribute_metadata = attribute_metadata)
  })
  
  out <- list(pkg_metadata = pkg_metadata,
              metadata_id = meta_id,
              data_objects = data_objects)
  class(out) <- "D1Data"
  
  return(out)
}

#' Writes D1Data objects to Disk
#'
#' @param D1Data (D1Data) A result of \code{get_D1Data}.
#' @param path (optional/character) Path to directory to write data to.
#' 
#' @importFrom dataone listFormats
#' @importFrom EML write_eml
#' @importFrom xml2 write_xml
#' @importFrom readr write_csv
#'
#' @examples
#' \dontrun{
#' obj <- "https://dx.doi.org/10.6073/pasta/edc06bbae6db80e801b6e52253f2ea09"
#' D1Data <- get_D1Data(obj)
#' write_D1Data(D1Data)
#' 
#' obj <- "https://cn.dataone.org/cn/v2/resolve/urn%3Auuid%3A085ffc05-6e5d-4d5d-9b8c-6763e4bf85ac" #DATA
#' D1Data <- get_D1Data(obj)
#' write_D1Data(D1Data)
#'}
#' @export
write_D1Data <- function(D1Data, path = getwd()) {
  
  stopifnot(class(D1Data) == "D1Data")
  stopifnot(dir.exists(path))
  
  file_pattern <- "[^a-zA-Z0-9\\.\\-]+"
  file_sub <- "_"
  
  ## Create package folder
  dir_name <- gsub(file_pattern, file_sub, D1Data$metadata_id)
  pkg_dir <- file.path(path, dir_name)
  dir.create(pkg_dir)

  ## Write package metadata
  path_pkg_metadata <- file.path(pkg_dir, "pkg_metadata.xml")
  if (attr(class(D1Data$pkg_metadata), "package") == "EML") {
    EML::write_eml(D1Data$pkg_metadata, path_pkg_metadata)
  
  } else {
      xml2::write_xml(D1Data$pkg_metadata, path_pkg_metadata)
  }
  
  ## Write D1Data rdata
  path_D1Data <- file.path(pkg_dir, "D1Data.rds")
  saveRDS(D1Data, path_D1Data)
  
  ## Write Data Objects
  formatIDs <- dataone::listFormats(dataone::CNode("PROD"))
  
  lapply(D1Data$data_objects, function(x) {
    dir_name <- gsub(file_pattern, file_sub, x$identifier)
    data_dir <- file.path(pkg_dir, dir_name)
    dir.create(data_dir)
    
    ## Write data
    if (!is.na(x$file_name)) {
      fileName <- x$file_name
      
      # If filename is not in sysmeta use the identifier with an extension determined by the formatID
    } else {
      fileName <- gsub(file_pattern, file_sub, x$identifier)
      
      if (x$data_format != "application/octet-stream") {
        Extension <- formatIDs$Extension[which(formatIDs$ID == x$data_format)]
        fileName <- paste0(fileName, ".", Extension)
      }
      
    }
    path_data <- file.path(data_dir, fileName)
    writeBin(x$data, path_data)
    
    ## Write data metadata
    if (!is.null(x$data_metadata)) {
    path_data_metadata <- file.path(data_dir, "data_metadata.xml")
    if (attr(class(x$data_metadata), "package") == "EML") {
      EML::write_eml(x$data_metadata, path_data_metadata)
      
    } else {
      xml2::write_xml(x$data_metadata, path_data_metadata)
    }
    }
    
    ## Write attribute metadata
    if (!is.null(x$attribute_metadata)) {
    path_attribute_metadata <- file.path(data_dir, "attribute_metadata.csv")
    readr::write_csv(x$attribute_metadata, path_attribute_metadata)
    }

    return(NULL)
  })
  
return(pkg_dir)
}
