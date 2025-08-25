CREATE TABLE tasks (uuid TEXT NOT NULL PRIMARY KEY, title TEXT, isCompleted INT NOT NULL DEFAULT 0);
CREATE TABLE tags (uuid TEXT NOT NULL PRIMARY KEY, name TEXT, UNIQUE(name));
CREATE TABLE tasks_tags (uuid TEXT NOT NULL PRIMARY KEY, task_uuid TEXT, tag_uuid TEXT, FOREIGN KEY (task_uuid) REFERENCES tasks(uuid), FOREIGN KEY (tag_uuid) REFERENCES tags(uuid));