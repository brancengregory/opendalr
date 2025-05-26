# tests/testthat/test-operator.R

# Helper to ensure we are using the package's functions, not base R ones if names collide
# This assumes your package is loaded, e.g. by devtools::load_all() or library(opendalr)
# during interactive testing, or by testthat's environment.
# For functions like path_exists, is_file, etc.
# If your package is named 'opendalr':
# path_exists <- opendalr::path_exists
# is_file <- opendalr::is_file
# ... and so on for all your exported functions.
# Alternatively, rely on testthat loading the package namespace.

# --- Setup a temporary directory for tests ---
# This ensures tests are isolated and don't affect the user's file system.
# withr::local_tempdir() is great as it handles cleanup automatically.
# If not using withr, you'd use setup and teardown blocks or manage temp dirs manually.

test_that("Operator creation and basic connectivity", {
  # Using withr for a temporary directory that's cleaned up
  temp_test_dir <- withr::local_tempdir(.local_envir = test_env())

  op <- connect_fs(root = temp_test_dir)
  expect_s3_class(op, "OpenDALOperator")

  # Test listing an empty directory (the temp dir itself)
  # Path "." is special and usually refers to the current context of the operator.
  expect_equal(dir_ls(op, "./"), "./")
})

test_that("File creation, existence, and type checking", {
  temp_test_dir <- withr::local_tempdir(.local_envir = test_env())
  op <- connect_fs(root = temp_test_dir)

  # Test path_exists for non-existent paths
  expect_false(path_exists(op, "test_file.txt"))
  expect_false(path_exists(op, "test_dir/")) # Check with trailing slash for consistency

  # Create a file
  file_write_text(op, "test_file.txt", "Hello OpenDAL!")
  expect_true(path_exists(op, "test_file.txt"))

  # Test is_file and is_dir for the created file
  expect_true(is_file(op, "test_file.txt"))
  expect_false(is_dir(op, "test_file.txt")) # A file is not a directory

  # Create a directory
  dir_create(op, "test_dir/") # Ensure trailing slash for creation
  expect_true(path_exists(op, "test_dir/"))
  expect_true(is_dir(op, "test_dir/"))   # Check with trailing slash
  expect_false(is_file(op, "test_dir/")) # A directory is not a file

  # Non-existent paths
  expect_false(is_file(op, "non_existent.txt"))
  expect_false(is_dir(op, "non_existent_dir/"))
})

test_that("File read/write operations (text and raw)", {
  temp_test_dir <- withr::local_tempdir(.local_envir = test_env())
  op <- connect_fs(root = temp_test_dir)

  # Text read/write
  test_string <- "Hello R and Rust! \U1F980" # Crab emoji for UTF-8 test
  file_write_text(op, "text_example.txt", test_string)
  read_string <- file_read_text(op, "text_example.txt")
  expect_equal(read_string, test_string)

  # Raw read/write
  test_raw <- as.raw(c(0x01, 0x02, 0x03, 0xAA, 0xFF))
  file_write_raw(op, "raw_example.bin", test_raw)
  read_raw_data <- file_read_raw(op, "raw_example.bin")
  expect_identical(read_raw_data, test_raw)

  # Overwrite
  file_write_text(op, "text_example.txt", "New content")
  expect_equal(file_read_text(op, "text_example.txt"), "New content")

  # Read non-existent file (should error)
  expect_error(file_read_text(op, "no_such_file.txt"))
})

test_that("File size and metadata", {
  temp_test_dir <- withr::local_tempdir(.local_envir = test_env())
  op <- connect_fs(root = temp_test_dir)

  # File with content
  content1 <- "12345"
  file_write_text(op, "size_file1.txt", content1)
  expect_equal(file_size(op, "size_file1.txt"), nchar(content1))

  meta1 <- path_stat(op, "size_file1.txt")
  expect_s3_class(meta1, "OpenDALMetadata")
  expect_true(meta1$is_file())
  expect_false(meta1$is_dir())
  expect_equal(as.numeric(meta1$content_length()), nchar(content1))

  # Empty file
  file_write_text(op, "empty_file.txt", "")
  expect_equal(file_size(op, "empty_file.txt"), 0)
  meta_empty <- path_stat(op, "empty_file.txt")
  expect_equal(as.numeric(meta_empty$content_length()), 0)

  # Directory
  dir_create(op, "meta_dir/") # Ensure trailing slash
  expect_true(is_na(file_size(op, "meta_dir/"))) # Check with trailing slash
  meta_dir <- path_stat(op, "meta_dir/") # Check with trailing slash
  expect_s3_class(meta_dir, "OpenDALMetadata")
  expect_true(meta_dir$is_dir())
  expect_false(meta_dir$is_file())
  # Content length for directories can be system/service dependent.
  # For local fs, it might be non-zero (e.g., size of directory entry itself).
  # Not strictly testing its value, just its type.

  # Non-existent
  expect_true(is_na(file_size(op, "no_file_here.txt")))
  expect_error(path_stat(op, "no_file_here.txt"))
  expect_error(path_stat(op, "no_dir_here/")) # Check non-existent dir with slash
})

test_that("Directory operations (create, list, delete)", {
  temp_test_dir <- withr::local_tempdir(.local_envir = test_env())
  op <- connect_fs(root = temp_test_dir)

  # Create directory
  dir_create(op, "my_dir/")
  expect_true(is_dir(op, "my_dir/"))

  # Create nested directories
  dir_create(op, "my_dir/subdir1/subdir2/")
  expect_true(is_dir(op, "my_dir/subdir1/subdir2/"))

  # List directory
  file_write_text(op, "my_dir/file1.txt", "f1")
  file_write_text(op, "my_dir/subdir1/file2.txt", "f2")

  listed_items_my_dir <- dir_ls(op, "my_dir/") # List with trailing slash
  # OpenDAL's list on "my_dir/" should return "file1.txt" and "subdir1/" (or just "subdir1")
  # The names returned by opendal `list` are usually just the final component.
  # If it returns "subdir1/", we might need to strip the trailing slash for comparison if needed.
  # For now, assume it returns names like "file1.txt" and "subdir1".
  # If opendal returns "subdir1/" for directories, adjust expectations.
  # Let's assume opendal's list returns "subdir1" for the directory entry.
  expect_true(all(c("file1.txt", "subdir1") %in% listed_items_my_dir))
  expect_length(listed_items_my_dir, 2)

  listed_items_subdir1 <- dir_ls(op, "my_dir/subdir1/") # List with trailing slash
  expect_true(all(c("file2.txt", "subdir2") %in% listed_items_subdir1))
  expect_length(listed_items_subdir1, 2)

  # List empty directory
  expect_equal(dir_ls(op, "my_dir/subdir1/subdir2/"), character(0)) # List with trailing slash

  # Delete file
  file_delete(op, "my_dir/file1.txt")
  expect_false(path_exists(op, "my_dir/file1.txt"))

  # Delete directory (recursive)
  dir_delete(op, "my_dir/") # Delete with trailing slash for consistency
  expect_false(path_exists(op, "my_dir/"))
  expect_false(path_exists(op, "my_dir/subdir1/")) # Check sub-paths also
  expect_false(path_exists(op, "my_dir/subdir1/file2.txt"))
})

test_that("File copy and move operations", {
  temp_test_dir <- withr::local_tempdir(.local_envir = test_env())
  op <- connect_fs(root = temp_test_dir)

  # File copy
  original_content <- "Content to be copied."
  file_write_text(op, "source.txt", original_content)

  file_copy(op, "source.txt", "destination.txt")
  expect_true(path_exists(op, "destination.txt"))
  expect_equal(file_read_text(op, "destination.txt"), original_content)

  # Attempt to copy non-existent file
  expect_error(file_copy(op, "no_source.txt", "wont_happen.txt"))

  # File move (rename)
  move_content <- "Content to be moved."
  file_write_text(op, "old_name.txt", move_content)
  path_move(op, "old_name.txt", "new_name.txt")
  expect_false(path_exists(op, "old_name.txt"))
  expect_true(path_exists(op, "new_name.txt"))
  expect_equal(file_read_text(op, "new_name.txt"), move_content)

  # Move directory
  dir_create(op, "old_dir_name/sub/") # Create with trailing slashes
  file_write_text(op, "old_dir_name/sub/file_in_moved_dir.txt", "data")

  # For path_move (rename), OpenDAL usually expects paths without trailing slashes
  # for the entries themselves, even if they are directories.
  # The trailing slash is more for operations *on* the directory (list, stat as dir).
  path_move(op, "old_dir_name", "new_dir_name") # Moving the entry "old_dir_name"

  expect_false(path_exists(op, "old_dir_name/")) # Check old path (with slash)
  expect_false(path_exists(op, "old_dir_name"))  # Check old path (without slash)

  expect_true(is_dir(op, "new_dir_name/")) # Check new path as directory (with slash)
  expect_true(path_exists(op, "new_dir_name/sub/file_in_moved_dir.txt"))
})

# --- Tests for Cloud Services (S3, GCS) ---
# These would typically be skipped on CRAN and in environments without credentials.
# They require actual buckets and credentials, or sophisticated mocking/emulators.

# Example structure for a conditional S3 test:
# test_that("S3 connection and basic list (conditional)", {
#   s3_bucket <- Sys.getenv("DALR_TEST_S3_BUCKET")
#   s3_region <- Sys.getenv("DALR_TEST_S3_REGION") # Or AWS_REGION etc.
#
#   # Skip if environment variables for testing are not set
#   skip_if_not(nzchar(s3_bucket) && nzchar(s3_region), "S3 test credentials/bucket not set")
#   # Also ensure necessary AWS CLI/SDK credentials are in the environment for OpenDAL
#   skip_if_no_envvar(c("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")) # Basic check
#
#   op_s3 <- tryCatch(
#     connect_s3(bucket = s3_bucket, region = s3_region, root = "opendalr_tests/"), # Use a root prefix
#     error = function(e) {
#       skip(paste("S3 connection failed:", conditionMessage(e)))
#     }
#   )
#   expect_s3_class(op_s3, "OpenDALOperator")
#
#   # Create a known temporary file/prefix for testing (now relative to operator root)
#   test_file_in_root <- "test_file.txt"
#   on.exit(try(file_delete(op_s3, test_file_in_root)), add = TRUE) # Cleanup
#   on.exit(try(dir_delete(op_s3, "")), add = TRUE) # Attempt to clean the root prefix if empty
#
#   file_write_text(op_s3, test_file_in_root, "Hello S3 from testthat")
#
#   items <- dir_ls(op_s3, ".") # List relative to operator's root
#   expect_true(test_file_in_root %in% items)
# })

