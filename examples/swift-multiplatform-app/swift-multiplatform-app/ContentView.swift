//
//  ContentView.swift
//  swift-multiplatform-app
//
//  Created by Gioele Cantoni on 18/08/25.
//

import SwiftUI

struct ContentView: View {
    @State private var statusLines: [String] = []
    private var statusText: String { statusLines.joined(separator: "\n") }

    var body: some View {
        VStack(spacing: 12) {
            Image(systemName: "globe")
                .imageScale(.large)
                .foregroundStyle(.tint)
            Text("Hello, world!")

            Divider()

            Text("Status")
                .font(.headline)

            ScrollView {
                Text(statusText.isEmpty ? "No status yet." : statusText)
                    .font(.system(.footnote, design: .monospaced))
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .textSelection(.enabled)
                    .padding(.vertical, 4)
            }
            .frame(maxHeight: 260)
        }
        .padding()
        .task {
            log("Starting...")
            var db: OpaquePointer?

            // Open an in-memory database just for demonstrating status updates.
            // Replace with your own URL/path if needed.
            var rc = sqlite3_open(":memory:", &db)
            if rc != SQLITE_OK {
                let msg = db.flatMap { sqlite3_errmsg($0) }.map { String(cString: $0) } ?? "Unknown error"
                log("sqlite3_open failed (\(rc)): \(msg)")
                if let db { sqlite3_close(db) }
                return
            }
            log("Database opened.")

            // Enable loadable extensions
            rc = sqlite3_enable_load_extension(db, 1)
            log("sqlite3_enable_load_extension rc=\(rc)")

            // Locate the extension in the bundle (adjust as needed)
            let vendorBundle = Bundle(identifier: "ai.sqlite.cloudsync")
            let candidatePaths: [String?] = [
                vendorBundle?.path(forResource: "CloudSync", ofType: "dylib"),
                vendorBundle?.path(forResource: "CloudSync", ofType: ""),
                Bundle.main.path(forResource: "CloudSync", ofType: "dylib"),
                Bundle.main.path(forResource: "CloudSync", ofType: "")
            ]
            let cloudsyncPath = candidatePaths.compactMap { $0 }.first
            log("cloudsyncPath: \(cloudsyncPath ?? "Not found")")

            var loaded = false
            if let path = cloudsyncPath {
                var errMsg: UnsafeMutablePointer<Int8>? = nil
                rc = sqlite3_load_extension(db, path, nil, &errMsg)
                if rc != SQLITE_OK {
                    let message = errMsg.map { String(cString: $0) } ?? String(cString: sqlite3_errmsg(db))
                    if let e = errMsg { sqlite3_free(e) }
                    log("sqlite3_load_extension failed rc=\(rc): \(message)")
                } else {
                    loaded = true
                    log("sqlite3_load_extension succeeded.")
                }

                // Optionally disable further extension loading
                _ = sqlite3_enable_load_extension(db, 0)
            } else {
                log("Skipping load: extension file not found in bundle.")
            }

            // Run SELECT cloudsync_version() and log the result
            if loaded {
                let sql = "SELECT cloudsync_version()"
                log("Running query: \(sql)")
                var stmt: OpaquePointer?
                rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
                if rc != SQLITE_OK {
                    let msg = String(cString: sqlite3_errmsg(db))
                    log("sqlite3_prepare_v2 failed (\(rc)): \(msg)")
                } else {
                    defer { sqlite3_finalize(stmt) }
                    rc = sqlite3_step(stmt)
                    if rc == SQLITE_ROW {
                        if let cstr = sqlite3_column_text(stmt, 0) {
                            let version = String(cString: cstr)
                            log("cloudsync_version(): \(version)")
                        } else {
                            log("cloudsync_version(): (null)")
                        }
                    } else if rc == SQLITE_DONE {
                        log("cloudsync_version() returned no rows")
                    } else {
                        let msg = String(cString: sqlite3_errmsg(db))
                        log("sqlite3_step failed (\(rc)): \(msg)")
                    }
                }
            } else {
                log("Extension not loaded; skipping cloudsync_version() query.")
            }

            if let db { sqlite3_close(db) }
            log("Done.")
        }
    }

    @MainActor
    private func log(_ line: String) {
        statusLines.append(line)
    }
}

#Preview {
    ContentView()
}
