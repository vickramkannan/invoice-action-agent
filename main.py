            role = message.get("role")
            if role != "ROLE_USER": return err(400, "INVALID_ROLE")
            media, data = part_data(message)
            if media == BATCH_MODE:
                batch_id = data.get("batchId")
                if not batch_id or not data.get("policyRevision"):
                    return err(400, "INVALID_BATCH")
                proposals = make_ai_proposals(data)
                task_id = "task_" + secrets.token_urlsafe(18)
                context_id = message.get("contextId") or "ctx_" + secrets.token_urlsafe(18)
                task = proto_task(task_id, context_id, "TASK_STATE_INPUT_REQUIRED", [message],
                                  [artifact(PROPOSAL_MODE, {"batchId": batch_id, "proposals": proposals})])
            elif media == RESULT_MODE:
                return await continue_results(message, data, principal, mid, digest)
            else:
                return err(415, "UNSUPPORTED_INPUT_MODE")
        except requests.RequestException:
            return err(502, "AI_PROVIDER_ERROR")
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": str(e) if str(e).isupper() else "INVALID_MESSAGE"})
        store_task(task, principal, batch_id if media == BATCH_MODE else None)
        with connect() as db:
            db.execute("INSERT INTO messages(principal,message_id,digest,task_id) VALUES (?,?,?,?)",
                       (principal, mid, digest, task["id"]))
        return JSONResponse({"task": task}, media_type=A2A_MEDIA)

async def continue_results(message, data, principal, mid, digest):
    task_id = message.get("taskId")
    context_id = message.get("contextId")
    if not task_id or not context_id:
        return err(400, "INVALID_CONTINUATION")
    with connect() as db:
        row = task_load(db, task_id, principal)
        if not row: return err(404, "TASK_NOT_FOUND")
        task = json.loads(row["task_json"])
        if task["contextId"] != context_id or row["batch_id"] != data.get("batchId"):
            return err(409, "CONTINUATION_MISMATCH")
        if task["status"]["state"] in ("TASK_STATE_COMPLETED", "TASK_STATE_CANCELED"):
            # A terminal replay returns the immutable task, unless message-id conflict was detected earlier.
            return JSONResponse({"task": task}, media_type=A2A_MEDIA)
        proposal_data = task["artifacts"][0]["parts"][0]["data"]
        proposals = {p["packageId"]: p for p in proposal_data["proposals"]}
        results = data.get("results")
        if not isinstance(results, list) or len(results) != len(proposals):
            return err(400, "INVALID_RESULTS")
        executions = []
        for result in results:
            p = proposals.get(result.get("packageId"))
            if not p or result.get("actionId") != p["actionId"] or result.get("action") != p["action"]:
                return err(409, "PROPOSAL_MISMATCH")
            if result.get("outcome") == "ACCEPTED":
                executions.append({"packageId": p["packageId"], "actionId": p["actionId"],
                                   "action": p["action"], "receiptNonce": result.get("receiptNonce"),
                                   "facts": p["facts"], "evidenceRefs": p["evidenceRefs"]})
            elif result.get("outcome") != "REJECTED":
                return err(400, "INVALID_OUTCOME")
        if any(not x["receiptNonce"] for x in executions):
            return err(400, "MISSING_RECEIPT_NONCE")
        updated_history = list(task.get("history", [])) + [message]
        task["history"] = updated_history
        task["artifacts"] = list(task["artifacts"]) + [artifact(RECEIPT_MODE, {"batchId": data["batchId"], "executions": executions})]
        task["status"] = {"state": "TASK_STATE_COMPLETED"}
        with connect() as db:
            # Compare-and-swap ensures cancel/result cannot both win.
            current = db.execute("SELECT task_json,state FROM tasks WHERE id=? AND principal=?", (task_id, principal)).fetchone()
            if not current or current["state"] != "TASK_STATE_INPUT_REQUIRED":
                return err(409, "TASK_STATE_CONFLICT")
            db.execute("UPDATE tasks SET state=?,task_json=?,updated_at=? WHERE id=? AND principal=? AND state=?",
                       ("TASK_STATE_COMPLETED", json.dumps(task, ensure_ascii=False), now(), task_id, principal, "TASK_STATE_INPUT_REQUIRED"))
            if db.total_changes < 1: return err(409, "TASK_STATE_CONFLICT")
            db.execute("INSERT INTO messages(principal,message_id,digest,task_id) VALUES (?,?,?,?)", (principal, mid, digest, task_id))
        return JSONResponse({"task": task}, media_type=A2A_MEDIA)

@app.get("/a2a/tasks/{task_id}")
def get_task(task_id: str, request: Request):
    h = check_headers(request)
    if h: return h
    principal = principal_from(request)
    if not principal: return err(401, "UNAUTHORIZED")
    with connect() as db:
        row = task_load(db, task_id, principal)
        if not row: return err(404, "TASK_NOT_FOUND")
        return JSONResponse(json.loads(row["task_json"]), media_type=A2A_MEDIA)

@app.get("/a2a/tasks")
def list_tasks(request: Request):
    h = check_headers(request)
    if h: return h
    principal = principal_from(request)
    if not principal: return err(401, "UNAUTHORIZED")
    with connect() as db:
        rows = db.execute("SELECT task_json FROM tasks WHERE principal=? ORDER BY created_at DESC", (principal,)).fetchall()
        return JSONResponse({"tasks": [json.loads(r["task_json"]) for r in rows]}, media_type=A2A_MEDIA)

@app.post("/a2a/tasks/{task_id}:cancel")
def cancel_task(task_id: str, request: Request):
    h = check_headers(request)
    if h: return h
    principal = principal_from(request)
    if not principal: return err(401, "UNAUTHORIZED")
    with LOCK:
        with connect() as db:
            row = task_load(db, task_id, principal)
            if not row: return err(404, "TASK_NOT_FOUND")
            task = json.loads(row["task_json"])
            if task["status"]["state"] in ("TASK_STATE_COMPLETED", "TASK_STATE_CANCELED"):
                return err(409, "TASK_TERMINAL")
            task["status"] = {"state": "TASK_STATE_CANCELED"}
            changed = db.execute("UPDATE tasks SET state=?,task_json=?,updated_at=? WHERE id=? AND principal=? AND state=?",
                                 ("TASK_STATE_CANCELED", json.dumps(task, ensure_ascii=False), now(), task_id, principal, row["state"])).rowcount
            if not changed: return err(409, "TASK_STATE_CONFLICT")
            return JSONResponse(task, media_type=A2A_MEDIA)

@app.get("/health")
def health():
    return {"ok": True, "version": APP_VERSION}
