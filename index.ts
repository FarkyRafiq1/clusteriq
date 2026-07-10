import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import {
  CLUSTERIQ_BASE_URL,
  CORS_HEADERS,
  isWorkspaceMember,
  parseBackendError,
  safeJsonParse,
} from "../_shared/clusteriq.ts";

// -------- helpers -----------------------------------------------------------

function serviceClient() {
  return createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );
}

function jsonResponse(body: unknown, status = 200, extraHeaders: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS_HEADERS, "Content-Type": "application/json", ...extraHeaders },
  });
}

function errorResponse(
  code: string,
  message: string,
  status: number,
  extras: Record<string, unknown> = {},
  extraHeaders: Record<string, string> = {}
) {
  return jsonResponse({ code, message, ...extras }, status, extraHeaders);
}

async function logEvent(params: {
  event_type: string;
  severity?: string;
  message?: string;
  metadata?: Record<string, unknown>;
  user_id?: string | null;
  workspace_id?: string | null;
  project_id?: string | null;
  duration_ms?: number | null;
  status_code?: number | null;
  endpoint?: string | null;
}) {
  try {
    await serviceClient().from("system_events").insert({
      event_type: params.event_type,
      severity: params.severity || "info",
      source: "cluster-proxy",
      message: params.message || "",
      metadata: params.metadata || {},
      user_id: params.user_id || null,
      workspace_id: params.workspace_id || null,
      project_id: params.project_id || null,
      duration_ms: params.duration_ms || null,
      status_code: params.status_code || null,
      endpoint: params.endpoint || null,
    });
  } catch (e) {
    console.error("Failed to log event:", e);
  }
}

/**
 * True if `userId` belongs to the workspace that owns `uploadId`. Resolves the
 * workspace from the upload row (falling back to the project) using the service
 * client, so a client-supplied workspaceId can never widen access.
 */
async function callerOwnsUpload(uploadId: string, userId: string): Promise<boolean> {
  const db = serviceClient();
  const { data: upload } = await db
    .from("uploads")
    .select("workspace_id, project_id")
    .eq("id", uploadId)
    .maybeSingle();
  if (!upload) return false;

  let workspaceId: string | null = (upload.workspace_id as string) ?? null;
  if (!workspaceId && upload.project_id) {
    const { data: proj } = await db
      .from("projects")
      .select("workspace_id")
      .eq("id", upload.project_id as string)
      .maybeSingle();
    workspaceId = (proj?.workspace_id as string) ?? null;
  }
  return isWorkspaceMember(db, workspaceId, userId);
}

async function authenticate(req: Request): Promise<{ userId: string } | Response> {
  const authHeader = req.headers.get("authorization");
  if (!authHeader?.startsWith("Bearer ")) {
    return errorResponse("UNAUTHORIZED", "Missing bearer token.", 401);
  }
  const supabaseAuth = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_ANON_KEY")!
  );
  const { data: { user }, error } = await supabaseAuth.auth.getUser(
    authHeader.replace("Bearer ", "")
  );
  if (error || !user) {
    return errorResponse("UNAUTHORIZED", "Invalid or expired session.", 401);
  }
  return { userId: user.id };
}

/**
 * Forward a backend error response to the caller with matching status,
 * Retry-After header, and structured JSON body.
 */
async function forwardBackendError(
  resp: Response,
  ctx: { userId: string; workspaceId?: string; projectId?: string; uploadId?: string; endpoint: string; startTime: number }
) {
  const err = await parseBackendError(resp);
  const extraHeaders: Record<string, string> = {};
  if (err.retry_after !== null && err.retry_after !== undefined) {
    extraHeaders["Retry-After"] = String(err.retry_after);
  }
  if (ctx.uploadId) {
    try {
      await serviceClient().from("uploads").update({ status: "failed" }).eq("id", ctx.uploadId);
    } catch { /* ignore */ }
  }
  await logEvent({
    event_type: "job_failed",
    severity: "error",
    message: `Backend ${err.code} (${resp.status}): ${err.message}`,
    endpoint: ctx.endpoint,
    status_code: resp.status,
    user_id: ctx.userId,
    workspace_id: ctx.workspaceId,
    project_id: ctx.projectId,
    duration_ms: Date.now() - ctx.startTime,
    metadata: { request_id: err.request_id, code: err.code },
  });
  return errorResponse(
    err.code,
    err.message,
    resp.status,
    {
      columns_detected: err.columns_detected,
      retry_after: err.retry_after,
      request_id: err.request_id,
    },
    extraHeaders
  );
}

/** Build a File from a base64-encoded upload sent via JSON. */
function fileFromBase64(b64: string, name: string, contentType: string): File {
  const bytes = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
  return new File([bytes], name, { type: contentType || "application/octet-stream" });
}

// -------- main handler ------------------------------------------------------

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: CORS_HEADERS });
  }

  const startTime = Date.now();
  const url = new URL(req.url);
  const action = (url.searchParams.get("action") ||
    (req.headers.get("content-type")?.includes("multipart/form-data") ? "submit" : "submit")) as
    | "preview" | "submit" | "poll" | "result" | "cancel";

  const auth = await authenticate(req);
  if (auth instanceof Response) return auth;
  const { userId } = auth;

  try {
    // ============ PREVIEW ==================================================
    if (action === "preview") {
      const { file, workspaceId, projectId } = await readMultipartOrJson(req);
      if (!file) return errorResponse("MISSING_FILE", "No file was uploaded.", 400);

      await logEvent({
        event_type: "api_request",
        message: "preview",
        endpoint: "/cluster-proxy/preview",
        user_id: userId,
        workspace_id: workspaceId ?? null,
        project_id: projectId ?? null,
      });

      const fd = new FormData();
      fd.append("file", file, file.name);
      const resp = await fetch(`${CLUSTERIQ_BASE_URL}/preview`, { method: "POST", body: fd });
      if (!resp.ok) {
        return forwardBackendError(resp, {
          userId, workspaceId, projectId, endpoint: "/cluster-proxy/preview", startTime,
        });
      }
      const payload = safeJsonParse(await resp.text());
      return jsonResponse(payload);
    }

    // ============ SUBMIT (multipart /cluster) ==============================
    if (action === "submit") {
      const {
        file,
        uploadId,
        projectId,
        workspaceId,
        columnMapping,
        parameters,
      } = await readMultipartOrJson(req);

      if (!file) return errorResponse("MISSING_FILE", "No file was uploaded.", 400);
      if (!columnMapping?.keyword_column) {
        return errorResponse("KEYWORD_COLUMN_NOT_FOUND", "Keyword column is required.", 400);
      }

      // Authorisation: the caller must belong to the workspace they are
      // submitting against, and (if given) the upload/project must live in it.
      // Otherwise a member of workspace A could consume workspace B's quota and
      // attach results to it.
      if (workspaceId) {
        if (!(await isWorkspaceMember(serviceClient(), workspaceId, userId))) {
          return errorResponse("FORBIDDEN", "Not a member of this workspace.", 403);
        }
        if (projectId) {
          const { data: proj } = await serviceClient()
            .from("projects")
            .select("workspace_id")
            .eq("id", projectId)
            .maybeSingle();
          if (proj && proj.workspace_id !== workspaceId) {
            return errorResponse("FORBIDDEN", "Project does not belong to this workspace.", 403);
          }
        }
      }

      // Quota check
      if (workspaceId) {
        const { data: quotaCheck } = await serviceClient().rpc("check_workspace_quota", {
          _workspace_id: workspaceId,
          _action: "job",
        });
        if (quotaCheck && !(quotaCheck as any).allowed) {
          return errorResponse(
            "QUOTA_EXCEEDED",
            (quotaCheck as any).reason || "Quota limit reached",
            429,
            { quota_exceeded: true }
          );
        }
      }

      await logEvent({
        event_type: "job_started",
        message: `Clustering started for ${file.name}`,
        metadata: { file_name: file.name, parameters },
        user_id: userId,
        workspace_id: workspaceId ?? null,
        project_id: projectId ?? null,
      });

      const fd = new FormData();
      fd.append("file", file, file.name);
      const mappingFields: Array<[string, string | undefined]> = [
        ["keyword_column", columnMapping.keyword_column],
        ["volume_column", columnMapping.volume_column],
        ["difficulty_column", columnMapping.difficulty_column],
        ["rank_column", columnMapping.rank_column],
        ["url_column", columnMapping.url_column],
      ];
      for (const [k, v] of mappingFields) {
        if (v) fd.append(k, v);
      }
      if (parameters && typeof parameters === "object") {
        const allowed = ["min_cluster_size", "max_cluster_size", "min_volume", "max_difficulty", "similarity_threshold", "url_grouping", "intent_filter"];
        for (const k of allowed) {
          const val = (parameters as Record<string, unknown>)[k];
          if (val !== undefined && val !== null) {
            fd.append(k, Array.isArray(val) ? JSON.stringify(val) : String(val));
          }
        }
      }

      const resp = await fetch(`${CLUSTERIQ_BASE_URL}/cluster`, { method: "POST", body: fd });
      if (!resp.ok) {
        return forwardBackendError(resp, {
          userId, workspaceId, projectId, uploadId, endpoint: "/cluster-proxy/submit", startTime,
        });
      }
      const result = safeJsonParse(await resp.text()) as Record<string, unknown>;

      // Async job flow
      if (result?.job_id) {
        if (workspaceId && projectId && uploadId) {
          await serviceClient().from("jobs").insert({
            workspace_id: workspaceId,
            project_id: projectId,
            upload_id: uploadId,
            user_id: userId,
            status: (result.status as string) || "queued",
            parameters: parameters || {},
            // Engine's own job id — lets any surface cancel the job later
            backend_job_id: String(result.job_id),
          });
        }
        return jsonResponse({ async: true, job_id: result.job_id, status: result.status || "queued" });
      }

      // Sync response — persist immediately.
      if (workspaceId) {
        const rowCount = Array.isArray(result?.rows) ? (result.rows as unknown[]).length : 0;
        await serviceClient().rpc("increment_workspace_usage", {
          _workspace_id: workspaceId,
          _uploads: 1,
          _jobs: 1,
          _keywords: rowCount,
          _storage_mb: file.size / (1024 * 1024),
          _exports: 0,
        });
      }
      const clusterCount = uploadId && projectId
        ? await persistResults(result, uploadId, projectId)
        : 0;
      await logEvent({
        event_type: "job_completed",
        message: `Sync clustering completed: ${clusterCount} clusters`,
        user_id: userId,
        workspace_id: workspaceId ?? null,
        project_id: projectId ?? null,
        duration_ms: Date.now() - startTime,
      });
      return jsonResponse({ async: false, success: true, clusterCount, summary: result.summary ?? null });
    }

    // ============ POLL =====================================================
    if (action === "poll") {
      const { jobId, uploadId, workspaceId, projectId } = await req.json();
      if (!jobId) return errorResponse("MISSING_JOB_ID", "Missing jobId.", 400);
      // Authorise against the upload's real workspace (never the client value).
      if (uploadId) {
        const ok = await callerOwnsUpload(uploadId, userId);
        if (!ok) return errorResponse("FORBIDDEN", "Not authorised for this upload.", 403);
      }
      const resp = await fetch(`${CLUSTERIQ_BASE_URL}/jobs/${jobId}`);
      if (!resp.ok) {
        // Restart-loss recovery: engine job state is in-process, so a Railway
        // restart mid-job makes the id unknown (404 JOB_NOT_FOUND). Mark the
        // DB row failed with a clear message so the frontend surfaces
        // "re-submit" instead of polling a ghost forever.
        if (resp.status === 404 && uploadId) {
          await serviceClient()
            .from("jobs")
            .update({
              status: "failed",
              error_message:
                "The clustering engine restarted while this job was running. Please re-submit the file.",
              completed_at: new Date().toISOString(),
            })
            .eq("upload_id", uploadId)
            .in("status", ["queued", "processing"]);
          await serviceClient().from("uploads").update({ status: "failed" }).eq("id", uploadId);
        }
        return forwardBackendError(resp, {
          userId, workspaceId, projectId, uploadId, endpoint: "/cluster-proxy/poll", startTime,
        });
      }
      const jobStatus = safeJsonParse(await resp.text()) as Record<string, unknown>;
      if (uploadId && typeof jobStatus?.status === "string") {
        const nextStatus = jobStatus.status as string;
        const nextProgress = typeof jobStatus.progress === "number" ? jobStatus.progress : null;
        const updatePayload: Record<string, unknown> = { status: nextStatus };
        if (nextProgress !== null) updatePayload.progress = nextProgress;
        if (nextStatus === "processing") updatePayload.started_at = new Date().toISOString();
        if (nextStatus === "completed") {
          updatePayload.progress = 100;
          updatePayload.completed_at = new Date().toISOString();
        }
        if (nextStatus === "failed") {
          updatePayload.error_message = (jobStatus.error_message as string) || "Clustering job failed";
          updatePayload.completed_at = new Date().toISOString();
          await serviceClient().from("uploads").update({ status: "failed" }).eq("id", uploadId);
        }
        await serviceClient()
          .from("jobs")
          .update(updatePayload)
          .eq("upload_id", uploadId)
          .in("status", ["queued", "processing"]);
      }
      return jsonResponse(jobStatus);
    }

    // ============ RESULT ===================================================
    if (action === "result") {
      const { jobId, uploadId, projectId, workspaceId } = await req.json();
      if (!jobId || !uploadId || !projectId) {
        return errorResponse("MISSING_FIELDS", "jobId, uploadId, and projectId are required.", 400);
      }
      // Authorise against the upload's real workspace (never the client value).
      if (!(await callerOwnsUpload(uploadId, userId))) {
        return errorResponse("FORBIDDEN", "Not authorised for this upload.", 403);
      }
      const resp = await fetch(`${CLUSTERIQ_BASE_URL}/jobs/${jobId}/result`);
      if (!resp.ok) {
        return forwardBackendError(resp, {
          userId, workspaceId, projectId, uploadId, endpoint: "/cluster-proxy/result", startTime,
        });
      }
      const result = safeJsonParse(await resp.text()) as Record<string, unknown>;
      const clusterCount = await persistResults(result, uploadId, projectId);
      await serviceClient()
        .from("jobs")
        .update({
          status: "completed",
          progress: 100,
          cluster_count: clusterCount,
          completed_at: new Date().toISOString(),
        })
        .eq("upload_id", uploadId)
        .in("status", ["queued", "processing"]);
      return jsonResponse({ success: true, clusterCount, summary: result.summary ?? null });
    }

    // ============ CANCEL ===================================================
    if (action === "cancel") {
      const { jobRowId, uploadId, backendJobId } = await req.json();
      if (!jobRowId && !uploadId) {
        return errorResponse("MISSING_FIELDS", "jobRowId or uploadId is required.", 400);
      }

      // Resolve the active job row via the service client, then authorise the
      // caller against the row's real workspace (never a client-supplied one).
      const db = serviceClient();
      let query = db
        .from("jobs")
        .select("id, workspace_id, upload_id, backend_job_id, status")
        .in("status", ["queued", "processing"])
        .limit(1);
      query = jobRowId ? query.eq("id", jobRowId) : query.eq("upload_id", uploadId);
      const { data: jobRows } = await query;
      const job = jobRows?.[0];

      if (!job) {
        // Nothing active to cancel — treat as success so the UI can settle.
        return jsonResponse({ success: true, canceled: false });
      }
      if (!(await isWorkspaceMember(db, job.workspace_id as string, userId))) {
        return errorResponse("FORBIDDEN", "Not authorised for this job.", 403);
      }

      // Ask the engine to stop. Engines without a cancel endpoint are
      // tolerated — the job is still marked canceled here and the poll loop
      // ignores rows that are no longer queued/processing.
      const engineJobId = (backendJobId as string) || (job.backend_job_id as string) || null;
      let engineCanceled = false;
      if (engineJobId) {
        try {
          let resp = await fetch(`${CLUSTERIQ_BASE_URL}/jobs/${engineJobId}`, { method: "DELETE" });
          if (resp.status === 404 || resp.status === 405) {
            resp = await fetch(`${CLUSTERIQ_BASE_URL}/jobs/${engineJobId}/cancel`, { method: "POST" });
          }
          engineCanceled = resp.ok;
        } catch (e) {
          console.error("Engine cancel failed:", e);
        }
      }

      await db
        .from("jobs")
        .update({
          status: "canceled",
          error_message: "Canceled by user",
          completed_at: new Date().toISOString(),
        })
        .eq("id", job.id)
        .in("status", ["queued", "processing"]);
      if (job.upload_id) {
        await db.from("uploads").update({ status: "failed" }).eq("id", job.upload_id);
      }

      await logEvent({
        event_type: "job_canceled",
        message: engineCanceled ? "Job canceled (engine acknowledged)" : "Job canceled (DB only)",
        endpoint: "/cluster-proxy/cancel",
        user_id: userId,
        workspace_id: job.workspace_id as string,
        duration_ms: Date.now() - startTime,
        metadata: { job_row_id: job.id, backend_job_id: engineJobId, engine_canceled: engineCanceled },
      });
      return jsonResponse({ success: true, canceled: true, engine_canceled: engineCanceled });
    }

    return errorResponse("UNKNOWN_ACTION", `Unknown action: ${action}`, 400);
  } catch (e) {
    console.error("cluster-proxy error:", e);
    await logEvent({
      event_type: "api_error",
      severity: "error",
      message: e instanceof Error ? e.message : "Clustering failed",
      endpoint: `/cluster-proxy/${action}`,
      status_code: 500,
      duration_ms: Date.now() - startTime,
    });
    return errorResponse("INTERNAL_ERROR", e instanceof Error ? e.message : "Clustering failed", 500);
  }
});

// -------- multipart or JSON body parsing -----------------------------------

async function readMultipartOrJson(req: Request): Promise<{
  file: File | null;
  uploadId?: string;
  projectId?: string;
  workspaceId?: string;
  columnMapping?: Record<string, string>;
  parameters?: Record<string, unknown>;
}> {
  const ct = req.headers.get("content-type") || "";
  if (ct.includes("multipart/form-data")) {
    const fd = await req.formData();
    const rawFile = fd.get("file");
    const file = rawFile instanceof File ? rawFile : null;
    const columnMappingRaw = fd.get("columnMapping");
    const parametersRaw = fd.get("parameters");
    return {
      file,
      uploadId: fd.get("uploadId")?.toString() || undefined,
      projectId: fd.get("projectId")?.toString() || undefined,
      workspaceId: fd.get("workspaceId")?.toString() || undefined,
      columnMapping: columnMappingRaw ? JSON.parse(columnMappingRaw.toString()) : undefined,
      parameters: parametersRaw ? JSON.parse(parametersRaw.toString()) : undefined,
    };
  }
  // JSON with base64 file payload — used as fallback / by supabase.functions.invoke
  const body = await req.json().catch(() => ({}));
  let file: File | null = null;
  if (body?.fileBase64 && body?.fileName) {
    file = fileFromBase64(body.fileBase64, body.fileName, body.fileType || "application/octet-stream");
  }
  return {
    file,
    uploadId: body.uploadId,
    projectId: body.projectId,
    workspaceId: body.workspaceId,
    columnMapping: body.columnMapping,
    parameters: body.parameters,
  };
}

// -------- persistence -------------------------------------------------------

function numOrNull(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

async function persistResults(
  result: Record<string, unknown>,
  uploadId: string,
  projectId: string
): Promise<number> {
  const db = serviceClient();
  const clusters = (result.clusters || []) as Record<string, unknown>[];
  const rows = (result.rows || []) as Record<string, unknown>[];

  const clustersToInsert = clusters.map((c, i) => ({
    upload_id: uploadId,
    project_id: projectId,
    job_id: (result as any).job_id || null,
    cluster_index: (typeof c.cluster_id === "number" ? c.cluster_id : i),
    cluster_slug: (c.cluster_slug as string) || (c.topic_label as string) || `cluster-${i}`,
    topic_label: (c.topic_label as string) || `Cluster ${i + 1}`,
    canonical_keyword: (c.canonical_keyword as string) || "",
    intent: (c.intent as string) || "",
    page_type: (c.page_type as string) || "",
    is_clustered: c.is_clustered === undefined ? true : Boolean(c.is_clustered),
    keyword_count: numOrNull(c.keyword_count) ?? 0,
    total_volume: numOrNull(c.total_volume) ?? 0,
    avg_difficulty: numOrNull(c.avg_difficulty),
    avg_position: numOrNull(c.avg_rank ?? c.avg_position),
    opportunity_score: numOrNull(c.opportunity_score) ?? 0,
    quality_score: numOrNull(c.cluster_quality ?? c.quality_score) ?? 0,
    quality_label: (c.quality_label as string) || "Fair",
    keywords: (c.keywords as unknown[]) || [],
    urls: (c.urls as unknown[]) || [],
  }));

  const { data: insertedClusters, error: clusterErr } = clustersToInsert.length
    ? await db.from("clusters").insert(clustersToInsert).select()
    : { data: [], error: null };
  if (clusterErr) throw clusterErr;

  // Map backend cluster_id -> DB uuid so we can attach rows.
  const idByIndex = new Map<number, string>();
  (insertedClusters || []).forEach((r: any, i: number) => {
    const backendId = typeof clusters[i]?.cluster_id === "number" ? clusters[i].cluster_id as number : i;
    idByIndex.set(backendId, r.id);
  });

  // Prefer the per-row `rows` array for keyword persistence; fall back to
  // cluster.keywords[] if the backend didn't provide `rows`.
  const keywordsToInsert: Record<string, unknown>[] = [];
  if (rows.length > 0) {
    for (const r of rows) {
      const backendId = typeof r.cluster_id === "number" ? r.cluster_id as number : -1;
      const cid = idByIndex.get(backendId);
      if (!cid) continue;
      keywordsToInsert.push({
        cluster_id: cid,
        keyword: (r.keyword as string) || "",
        volume: numOrNull(r.volume),
        difficulty: numOrNull(r.difficulty),
        position: numOrNull(r.position),
        url: (r.url as string) || "",
        intent: (r.intent as string) || null,
        page_type: (r.page_type as string) || null,
        canonical_keyword: (r.canonical_keyword as string) || null,
        topic_label: (r.topic_label as string) || null,
        cluster_quality: numOrNull(r.cluster_quality),
        opportunity_score: numOrNull(r.opportunity_score),
        is_clustered: r.is_clustered === undefined ? true : Boolean(r.is_clustered),
        is_canonical: (r.canonical_keyword as string) === (r.keyword as string),
      });
    }
  } else {
    (insertedClusters || []).forEach((inserted: any, idx: number) => {
      const kws = (clusters[idx]?.keywords || []) as Record<string, unknown>[];
      kws.forEach((kw) => {
        keywordsToInsert.push({
          cluster_id: inserted.id,
          keyword: (kw.keyword as string) || "",
          volume: numOrNull(kw.volume),
          difficulty: numOrNull(kw.difficulty),
          position: numOrNull(kw.position ?? kw.rank),
          url: (kw.url as string) || "",
          is_canonical: Boolean(kw.is_canonical),
        });
      });
    });
  }

  if (keywordsToInsert.length > 0) {
    // Chunk to keep individual inserts reasonable.
    const CHUNK = 500;
    for (let i = 0; i < keywordsToInsert.length; i += CHUNK) {
      const { error: kwErr } = await db
        .from("cluster_keywords")
        .insert(keywordsToInsert.slice(i, i + CHUNK));
      if (kwErr) console.error("Keyword insert error:", kwErr);
    }
  }

  await db.from("uploads").update({ status: "complete" }).eq("id", uploadId);
  return insertedClusters?.length || 0;
}
