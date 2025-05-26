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
