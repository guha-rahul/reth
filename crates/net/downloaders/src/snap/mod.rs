//! Snap sync downloader implementation.
//!
//! This module provides a [`SnapDownloader`] that implements [`Stream`] to download
//! account state, storage, and bytecode using the snap protocol.

mod downloader;

pub use downloader::{SnapBatch, SnapDownloader, SnapDownloaderBuilder};
