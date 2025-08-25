# Swift Multiplatform App Example

A simple Swift App QuickStart with the SQLite CloudSync extension loading. Now you can build cross-platform apps that sync data seamlessly across devices.

## 🚀 Quick Start

1. Open Xcode project

2. Download the latest version of `cloudsync-apple-xcframework` from:  
   https://github.com/sqliteai/sqlite-sync/releases

3. In Xcode, click on your project name in the source tree (top left with the Xcode logo)

4. In the new tab that opens, navigate to the left column under the **Targets** section and click on the first target

5. You should now be in the **General** tab. Scroll down to **"Frameworks, Libraries, and Embedded Content"**

6. Click the old `CloudSync.xcframework` framework and press the **-** button

7. Click the **+** button → **Add Other...** → **Add Files...**

8. Select the downloaded `CloudSync.xcframework` folder

9. Switch to the **Build Phases** tab and verify that `CloudSync.xcframework` appears under **Embedded Frameworks**

## Handle Security Permissions (macOS)

When you return to the main ContentView file, you may encounter an Apple security error:

1. Click **Done** when the security dialog appears
2. Open **System Settings** → **Privacy & Security**
3. Scroll to the bottom and find the message "Mac blocked CloudSync"
4. Click **Allow Anyway**
5. Close and reopen ContentView in Xcode
6. The same error should appear but now with a third button **Open Anyway** - click it
7. If errors persist, try reopening and closing ContentView multiple times or repeat the security steps above

## Test the Setup

To verify that the extension loads correctly in your Swift project, run it for iOS or macOS.

## Expected Results

When you run the test app, you should see status messages in the UI indicating:
- Database connection success
- Extension loading status
- CloudSync version information (if successfully loaded)

<img width="640" height="400" src="https://github.com/user-attachments/assets/d37d65c0-7465-4547-9831-63ea2da2849d" />


This confirms that CloudSync is properly integrated and functional in your Swift project.
