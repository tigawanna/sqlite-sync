import { useState, useEffect, useCallback } from "react";
import { db } from "../db/dbConnection";
import { randomUUID } from 'expo-crypto';
import { useSyncContext } from '../components/SyncContext';

const useTasks = (tag = null) => {
  const [taskList, setTaskList] = useState([]);
  const { registerRefreshCallback } = useSyncContext();

  const getTasks = useCallback(async () => {
    try {
      let result;
      if (tag) {
        result = await db.execute(
          `
          SELECT tasks.*, tags.uuid AS tag_uuid, tags.name AS tag_name 
          FROM tasks 
          JOIN tasks_tags ON tasks.uuid = tasks_tags.task_uuid 
          JOIN tags ON tags.uuid = tasks_tags.tag_uuid 
          WHERE tag_name=?`,
          [tag]
        );
        setTaskList(result.rows);
      } else {
        result = await db.execute(`
          SELECT tasks.*, tags.uuid AS tag_uuid, tags.name AS tag_name 
          FROM tasks 
          JOIN tasks_tags ON tasks.uuid = tasks_tags.task_uuid 
          JOIN tags ON tags.uuid = tasks_tags.tag_uuid`);
        setTaskList(result.rows);
      }
    } catch (error) {
      console.error("Error getting tasks", error);
    }
  }, [tag]);

  const updateTask = async (completedStatus, taskUuid) => {
    try {
      await db.execute("UPDATE tasks SET isCompleted=? WHERE uuid=?", [completedStatus, taskUuid]);
      db.execute('SELECT cloudsync_network_send_changes();')
      setTaskList(prevTasks =>
        prevTasks.map(task =>
          task.uuid === taskUuid ? { ...task, isCompleted: completedStatus } : task
        )
      );
    } catch (error) {
      console.error("Error updating tasks", error);
    }
  };

  const addTaskTag = async (newTask, tag) => {
    try {
      if (tag.uuid) {
        const addNewTask = await db.execute("INSERT INTO tasks (uuid, title, isCompleted) VALUES (?, ?, ?) RETURNING *", [randomUUID(), newTask.title, newTask.isCompleted]);
        addNewTask.rows[0].tag_uuid = tag.uuid;
        addNewTask.rows[0].tag_name = tag.name;
        setTaskList([...taskList, addNewTask.rows[0]]);
        await db.execute("INSERT INTO tasks_tags (uuid, task_uuid, tag_uuid) VALUES (?, ?, ?)", [randomUUID(), addNewTask.rows[0].uuid, tag.uuid]);
      } else {
        const addNewTaskNoTag = await db.execute("INSERT INTO tasks (uuid, title, isCompleted) VALUES (?, ?, ?) RETURNING *", [randomUUID(), newTask.title, newTask.isCompleted]);
        setTaskList([...taskList, addNewTaskNoTag.rows[0]]);
      }
      db.execute('SELECT cloudsync_network_send_changes();')
    } catch (error) {
      console.error("Error adding task to database", error);
    }
  };

  const deleteTask = async (taskUuid) => {
    try {
      await db.execute("DELETE FROM tasks_tags WHERE task_uuid=?", [taskUuid]);
      await db.execute("DELETE FROM tasks WHERE uuid=?", [taskUuid]);
      db.execute('SELECT cloudsync_network_send_changes();')
      setTaskList(taskList.filter(task => task.uuid !== taskUuid));
    } catch (error) {
      console.error("Error deleting task", error);
    }
  };

  useEffect(() => {
    getTasks();
  }, [getTasks]);

  useEffect(() => {
    return registerRefreshCallback(() => {
      getTasks();
    });
  }, [registerRefreshCallback, getTasks]);

  return {
    taskList,
    updateTask,
    addTaskTag,
    deleteTask,
    refreshTasks: getTasks,
  };
};

export default useTasks;
