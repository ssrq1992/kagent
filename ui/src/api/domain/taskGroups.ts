/** AX capacity choices, obtained through kagent authorization. */
export interface TaskGroupChoice {
  namespace: string;
  name: string;
  uid: string;
  phase: string;
  replicas: number;
}
export interface TaskGroupPage {
  groups: TaskGroupChoice[];
  nextPageToken?: string;
}
