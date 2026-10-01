import { apiClient } from "../client";
import type { TaskGroupChoice } from "../domain/taskGroups";
import { type ApiResource, useApiResource } from "./useApiResource";

export function useTaskGroups(namespace: string): ApiResource<TaskGroupChoice[]> {
  return useApiResource(namespace ? ["taskGroups.list", namespace] : null, async () => {
    const groups: TaskGroupChoice[] = [];
    const seen = new Set<string>();
    let pageToken: string | undefined;
    do {
      const page = await apiClient.taskGroups.list({ namespace, pageToken, limit: 100 });
      groups.push(...page.groups);
      pageToken = page.nextPageToken;
      if (pageToken && seen.has(pageToken)) throw new Error("TaskGroup pagination repeated a token.");
      if (pageToken) seen.add(pageToken);
    } while (pageToken);
    return groups;
  });
}
