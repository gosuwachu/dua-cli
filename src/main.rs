#![forbid(unsafe_code)]
use anyhow::Result;
use clap::Parser;
use dua::TraversalSorting;
use dua::JWalkWalker;
use std::{fs, io, io::Write, path::PathBuf, process};
use owo_colors::{AnsiColors as Color, OwoColorize};
use std::path::Path;

mod crossdev;
#[cfg(any(feature = "tui-unix", feature = "tui-crossplatform"))]
mod interactive;
mod options;

fn stderr_if_tty() -> Option<io::Stderr> {
    if atty::is(atty::Stream::Stderr) {
        Some(io::stderr())
    } else {
        None
    }
}

fn main() -> Result<()> {
    use options::Command::*;

    let opt: options::Args = options::Args::parse_from(wild::args_os());
    let walk_options = dua::WalkOptions {
        threads: opt.threads,
        byte_format: opt.format.into(),
        apparent_size: opt.apparent_size,
        count_hard_links: opt.count_hard_links,
        sorting: TraversalSorting::None,
        cross_filesystems: !opt.stay_on_filesystem,
        ignore_dirs: opt.ignore_dirs,
    };
    let walker = JWalkWalker {
        options: walk_options.clone(),
    };
    let res = match opt.command {
        #[cfg(any(feature = "tui-unix", feature = "tui-crossplatform"))]
        Some(Interactive { input }) => {
            use crate::interactive::{Interaction, TerminalApp};
            use anyhow::{anyhow, Context};
            use crosstermion::terminal::{tui::new_terminal, AlternateRawScreen};

            let no_tty_msg = "Interactive mode requires a connected terminal";
            if atty::isnt(atty::Stream::Stderr) {
                return Err(anyhow!(no_tty_msg));
            }

            let mut terminal = new_terminal(
                AlternateRawScreen::try_from(io::stderr()).with_context(|| no_tty_msg)?,
            )
            .with_context(|| "Could not instantiate terminal")?;
            let res = TerminalApp::initialize(
                &mut terminal,
                walk_options,
                paths_from(input, !opt.stay_on_filesystem)?,
                Interaction::Full,
            )?
            .map(|(keys_rx, mut app)| {
                let res = app.process_events(&mut terminal, keys_rx.into_iter());

                let res = res.map(|r| {
                    (
                        r,
                        app.window
                            .mark_pane
                            .take()
                            .map(|marked| marked.into_paths()),
                    )
                });
                // Leak app memory to avoid having to wait for the hashmap to deallocate,
                // which causes a noticeable delay shortly before the the program exits anyway.
                std::mem::forget(app);
                res
            });

            drop(terminal);
            io::stderr().flush().ok();

            // Exit 'quickly' to avoid having to not have to deal with slightly different types in the other match branches
            std::process::exit(
                res.transpose()?
                    .map(|(walk_result, paths)| {
                        if let Some(paths) = paths {
                            for path in paths {
                                println!("{}", path.display())
                            }
                        }
                        walk_result.to_exit_code()
                    })
                    .unwrap_or(0),
            );
        }
        Some(Aggregate {
            input,
            no_total,
            no_sort,
            statistics,
        }) => {
            let mut stdout = io::stdout();

            let (res, stats, aggregates) = dua::aggregate(
                stderr_if_tty(),
                walker,
                &walk_options,
                !no_sort,
                paths_from(input, !opt.stay_on_filesystem)?,
            )?;

            for (path, num_bytes, num_errors) in aggregates.into_iter() {
                output_colored_path(
                    &mut stdout,
                    &walk_options,
                    &path,
                    num_bytes,
                    num_errors,
                    path_color_of(&path),
                )?;
            }
        
            if res.num_roots > 1 && !no_total {
                output_colored_path(
                    &mut stdout,
                    &walk_options,
                    Path::new("total"),
                    res.total,
                    res.num_errors,
                    None,
                )?;
            }

            if statistics {
                writeln!(io::stderr(), "{:?}", stats).ok();
            }
            res
        }
        None => {
            let mut stdout = io::stdout();
            
            let (res, _, aggregates) = dua::aggregate(
                stderr_if_tty(),
                walker,
                &walk_options,
                true,
                paths_from(opt.input, !opt.stay_on_filesystem)?,
            )?;

            for (path, num_bytes, num_errors) in aggregates.into_iter() {
                output_colored_path(
                    &mut stdout,
                    &walk_options,
                    &path,
                    num_bytes,
                    num_errors,
                    path_color_of(&path),
                )?;
            }
        
            if res.num_roots > 1 {
                output_colored_path(
                    &mut stdout,
                    &walk_options,
                    Path::new("total"),
                    res.total,
                    res.num_errors,
                    None,
                )?;
            }

            res
        }
    };

    process::exit(res.to_exit_code());
}

fn path_color_of(path: impl AsRef<Path>) -> Option<Color> {
    (!path.as_ref().is_file()).then_some(Color::Cyan)
}

fn output_colored_path(
    out: &mut impl io::Write,
    options: &dua::WalkOptions,
    path: impl AsRef<Path>,
    num_bytes: u128,
    num_errors: u64,
    path_color: Option<Color>,
) -> std::result::Result<(), io::Error> {
    let size = options.byte_format.display(num_bytes).to_string();
    let size = size.green();
    let size_width = options.byte_format.width();
    let path = path.as_ref().display();

    let errors = (num_errors != 0)
        .then(|| {
            let plural_s = if num_errors > 1 { "s" } else { "" };
            format!("  <{num_errors} IO Error{plural_s}>")
        })
        .unwrap_or_default();

    if let Some(color) = path_color {
        writeln!(out, "{size:>size_width$} {}{errors}", path.color(color))
    } else {
        writeln!(out, "{size:>size_width$} {path}{errors}")
    }
}

fn paths_from(paths: Vec<PathBuf>, cross_filesystems: bool) -> Result<Vec<PathBuf>, io::Error> {
    let device_id = std::env::current_dir()
        .ok()
        .and_then(|cwd| crossdev::init(&cwd).ok());

    if paths.is_empty() {
        cwd_dirlist().map(|paths| match device_id {
            Some(device_id) if !cross_filesystems => paths
                .into_iter()
                .filter(|p| match p.metadata() {
                    Ok(meta) => crossdev::is_same_device(device_id, &meta),
                    Err(_) => true,
                })
                .collect(),
            _ => paths,
        })
    } else {
        Ok(paths)
    }
}

fn cwd_dirlist() -> Result<Vec<PathBuf>, io::Error> {
    let mut v: Vec<_> = fs::read_dir(".")?
        .filter_map(|e| {
            e.ok()
                .and_then(|e| e.path().strip_prefix(".").ok().map(ToOwned::to_owned))
        })
        .filter(|p| {
            if let Ok(meta) = p.symlink_metadata() {
                if meta.file_type().is_symlink() {
                    return false;
                }
            };
            true
        })
        .collect();
    v.sort();
    Ok(v)
}
