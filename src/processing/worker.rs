use std::time::Duration;

use anyhow::{anyhow, bail};
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::logic::{get_file_task_from_queue, get_local_store, update_task_data};
use crate::processing::{cheaply_process_pdf_path, process_marker_pdf, process_pdf};
use crate::types::{DocStatus, FileStoreImplementation, MarkdownConversionMethod, ProcessingStage};
use tracing::{error, info};

static PDF_SEMAPHORE: Semaphore = Semaphore::const_new(3);

/// Start the worker that continuously processes PDF tasks from the queue.
pub async fn start_worker() {
    info!("Starting pdf processing worker.");
    let mut no_pdf_counter = 0;
    loop {
        let permit = PDF_SEMAPHORE.acquire().await;
        match get_file_task_from_queue().await {
            Some(status) => {
                no_pdf_counter = 0;
                tokio::spawn(async move {
                    if let Err(err) = process_pdf_from_status(status).await {
                        error!(%err, "encountered error processing pdf.");
                    }
                    drop(permit);
                });
            }
            None => {
                // No tasks available, sleep briefly
                no_pdf_counter += 1;
                if no_pdf_counter >= 30 {
                    info!("No pdfs detected after {no_pdf_counter} polls");
                    no_pdf_counter = 0;
                }
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn process_pdf_from_status(mut status: DocStatus) -> anyhow::Result<()> {
    async fn task_errored(mut status: DocStatus, err: anyhow::Error) -> anyhow::Error {
        status.error = Some("Encountered error: ".to_string() + &err.to_string());
        status.status = ProcessingStage::Errored;
        let _ = update_task_data(status).await;
        err
    }
    // Download the file
    let task_id = status.request_id;
    status.status = ProcessingStage::Processing;
    if let Err(err) = update_task_data(status.clone()).await {
        bail!("Failed to set status to Processing for task {task_id}: {err}",);
    }
    info!(task_id, "Updated document to processing stage.");

    let store = get_local_store();
    let download_result = store
        .file_store
        .download_to_file(&status.file_location)
        .await;
    if let Err(err) = download_result {
        return Err(task_errored(status, err.into()).await);
    }
    let local_path = download_result.unwrap();

    // Process PDF to markdown
    info!(
        local_path=%local_path.to_string_lossy(),
        "Downloaded result successfully, processing pdf on locally",
    );
    let local_path_str: &str = (&local_path).as_path().to_str().unwrap();

    // Update status based on processing result
    match process_pdf(local_path_str, &status.conversion_method).await {
        Ok(markdown) => {
            status.markdown = Some(markdown);
            status.status = ProcessingStage::Completed;
            info!(task_id, "Successfully processed pdf");
            match update_task_data(status).await {
                Ok(_) => Ok(()),
                Err(err) => {
                    bail!(
                        "Encountered error pushing final data to db: ".to_string()
                            + &err.to_string()
                    )
                }
            }
        }
        Err(err) => {
            tracing::error!(%err,task_id,"Encountered error processing pdf");
            Err(task_errored(status, anyhow!("Encountered error processing pdf: {err}")).await)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::get_task_data_from_id;
    use crate::types::FileLocation;

    // Isolate the global store and provide synthetic configuration in a child
    // test process. LocalPath never contacts S3; no process-global env mutation.
    #[tokio::test]
    async fn unsupported_ocr_records_error_in_status_store() {
        const CHILD: &str = "CRIMSON_OCR_REGRESSION_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "processing::worker::tests::unsupported_ocr_records_error_in_status_store",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("S3_ACCESS_KEY", "synthetic-test-only")
                .env("S3_SECRET_KEY", "synthetic-test-only")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        for (id, method) in [
            (u64::MAX - 1, MarkdownConversionMethod::OlmOcr),
            (u64::MAX - 2, MarkdownConversionMethod::Marker),
        ] {
            let status = DocStatus::new_from_id_loc(
                id,
                FileLocation::LocalPath("unused-synthetic.pdf".into()),
                method,
            );
            assert!(process_pdf_from_status(status).await.is_err());
            let recorded = get_task_data_from_id(id).await.unwrap();
            assert_eq!(recorded.status, ProcessingStage::Errored);
            assert_eq!(recorded.request_id, id);
            assert!(recorded.error.unwrap().contains("Not Implemented"));
            assert!(recorded.markdown.is_none());
        }
    }
}
