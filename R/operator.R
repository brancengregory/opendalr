#' S3 print method for OpenDALMetadata objects
#'
#' @param x An OpenDALMetadata object.
#' @param ... Further arguments passed to or from other methods (unused).
#' @export
#' @keywords internal
print.OpenDALMetadata <- function(x, ...) {
  cat("<OpenDALMetadata>\n")
  if (!is.null(x)) { # Ensure the object itself isn't NULL
    # Use the accessor methods to get data
    # Handle potential NULLs from accessors gracefully

    is_file_val <- tryCatch(x$is_file(), error = function(e) NULL)
    is_dir_val <- tryCatch(x$is_dir(), error = function(e) NULL)
    content_length_val <- tryCatch(x$content_length(), error = function(e) NULL)
    content_type_val <- tryCatch(x$content_type(), error = function(e) NULL) # Assuming you added this
    last_modified_val <- tryCatch(x$last_modified(), error = function(e) NULL) # Assuming you added this
    etag_val <- tryCatch(x$etag(), error = function(e) NULL) # Assuming you added this

    if (!is.null(is_file_val)) {
      cat("  Is File:          ", is_file_val, "\n")
    }
    if (!is.null(is_dir_val)) {
      cat("  Is Directory:     ", is_dir_val, "\n")
    }
    if (!is.null(content_length_val)) {
      cat("  Content Length:   ", content_length_val, " bytes\n")
    }
    if (!is.null(content_type_val) && !is.null(content_type_val[[1]])) { # Check if the Robj itself is not NULL and its content
      cat("  Content Type:     ", content_type_val, "\n")
    }
    if (!is.null(last_modified_val) && !is.null(last_modified_val[[1]])) {
      # Assuming last_modified_val is POSIXct or a numeric timestamp from Rust
      cat("  Last Modified:    ", format(last_modified_val), "\n")
    }
    if (!is.null(etag_val) && !is.null(etag_val[[1]])) {
      cat("  ETag:             ", etag_val, "\n")
    }
    # Add other fields like cache_control, content_disposition, content_range etc.
    # For content_range (if it's a list):
    cr_val <- tryCatch(x$content_range(), error = function(e) NULL)
    if (!is.null(cr_val) && is.list(cr_val)) {
      cat("  Content Range:\n")
      if (!is.null(cr_val$start)) cat("    Start: ", cr_val$start, "\n")
      if (!is.null(cr_val$end)) cat("    End:   ", cr_val$end, "\n")
      if (!is.null(cr_val$size)) cat("    Size:  ", cr_val$size, "\n")
    }

  } else {
    cat("  NULL\n")
  }
  invisible(x) # Standard practice for print methods
}

#' S3 print method for OpenDALOperator objects
#'
#' @param x An OpenDALOperator object.
#' @param ... Further arguments passed to or from other methods (unused).
#' @export
#' @keywords internal
print.OpenDALOperator <- function(x, ...) {
  cat("<OpenDALOperator>\n")
  if (!is.null(x)) {
    info <- NULL #tryCatch(x$info(), error = function(e) NULL) # Call the $info() method
    if (!is.null(info) && is.list(info)) {
      cat("  Scheme:           ", info$scheme, "\n")
      cat("  Root:             ", info$root, "\n")
      # You could add more info if $info() provides it
    } else {
      cat("  (Info not available or error fetching)\n")
    }
  } else {
    cat("  NULL\n")
  }
  invisible(x)
}

#' @export
connect_fs <- function(root = NULL) {
  OpenDALOperator$new_fs(root = root)
}

#' @export
path_exists <- function(operator, path) {
  operator$exists(path)
}

#' @export
path_stat <- function(operator, path) {
  operator$stat(path)
}

#' @export
is_file <- function(operator, path) {
  meta <- operator$stat(path)
  meta$is_file()
}

#' @export
is_dir <- function(operator, path) {
  meta <- operator$stat(path)
  meta$is_dir()
}

#' @export
file_size <- function(operator, path) {
  meta <- operator$stat(path)
  meta$content_length()
}

#' @export
file_read_raw <- function(operator, path) {
  operator$read(path)
}

#' @export
file_read_text <- function(operator, path, encoding = "UTF-8") {
  raw_content <- operator$read(path)
  rawToChar(raw_content)
}

#' @export
file_write_raw <- function(operator, path, data) {
  operator$write(path, data)
}

#' @export
file_write_text <- function(operator, path, text, encoding = "UTF-8") {
  raw_data <- charToRaw(enc2utf8(text))
  operator$write(path, raw_data)
}

#' @export
file_delete <- function(operator, path) {
  operator$delete(path)
}

#' @export
dir_create <- function(operator, path) {
  operator$create_dir(path)
}

#' @export
dir_delete <- function(operator, path) {
  operator$remove_all(path)
}

#' @export
dir_ls <- function(operator, path) {
  operator$list(path)
}

#' @export
file_copy <- function(operator, source_path, destination_path) {
  operator$copy(source_path, destination_path)
}

#' @export
path_move <- function(operator, source_path, destination_path) {
  operator$rename(old_path, new_path)
}
