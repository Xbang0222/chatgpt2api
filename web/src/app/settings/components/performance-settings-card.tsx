"use client";

import { Activity, Gauge, LoaderCircle, Save, ServerCog } from "lucide-react";
import { useEffect, useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { fetchSystemInfo, type SystemInfo } from "@/lib/api";

import { useSettingsStore } from "../store";

function displayAuto(value: unknown, effective: number) {
  const text = String(value ?? "").trim();
  return text ? text : `自动 (${effective || "-"})`;
}

export function PerformanceSettingsCard() {
  const [systemInfo, setSystemInfo] = useState<SystemInfo | null>(null);
  const [isLoadingInfo, setIsLoadingInfo] = useState(false);
  const config = useSettingsStore((state) => state.config);
  const isLoadingConfig = useSettingsStore((state) => state.isLoadingConfig);
  const isSavingConfig = useSettingsStore((state) => state.isSavingConfig);
  const setMaxImageWorkers = useSettingsStore((state) => state.setMaxImageWorkers);
  const setStarlettePoolSize = useSettingsStore((state) => state.setStarlettePoolSize);
  const saveConfig = useSettingsStore((state) => state.saveConfig);

  useEffect(() => {
    let cancelled = false;

    const load = async (silent = false) => {
      if (!silent) {
        setIsLoadingInfo(true);
      }
      try {
        const data = await fetchSystemInfo();
        if (!cancelled) {
          setSystemInfo(data);
        }
      } catch (error) {
        if (!silent) {
          toast.error(error instanceof Error ? error.message : "加载系统状态失败");
        }
      } finally {
        if (!cancelled && !silent) {
          setIsLoadingInfo(false);
        }
      }
    };

    void load();
    const timer = window.setInterval(() => void load(true), 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  const imageEffective = systemInfo?.image_workers.effective || Number(config?.max_image_workers_effective || 0);
  const starletteEffective = systemInfo?.starlette_pool.effective || Number(config?.starlette_pool_size_effective || 0);
  const currentInflight = systemInfo?.image_workers.current_inflight ?? 0;
  const rejectionCount = systemInfo?.image_workers.queue_full_rejections_24h ?? 0;
  const accountTotal = systemInfo?.accounts.total ?? 0;
  const perAccount = systemInfo?.accounts.per_account_concurrency ?? Number(config?.image_account_concurrency || 3);

  if (isLoadingConfig || !config) {
    return (
      <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
        <CardContent className="flex items-center justify-center p-10">
          <LoaderCircle className="size-5 animate-spin text-stone-400" />
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="rounded-2xl border-white/80 bg-white/90 shadow-sm">
      <CardContent className="space-y-5 p-6">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <div className="flex items-center gap-2 text-sm font-medium text-stone-900">
              <Gauge className="size-4 text-stone-500" />
              性能
            </div>
            <p className="mt-1 text-xs text-stone-500">线程池相关配置保存后，需要重启服务才会生效。</p>
          </div>
          <Button
            className="h-10 rounded-xl bg-stone-950 px-5 text-white hover:bg-stone-800"
            onClick={() => void saveConfig()}
            disabled={isSavingConfig}
          >
            {isSavingConfig ? <LoaderCircle className="size-4 animate-spin" /> : <Save className="size-4" />}
            保存
          </Button>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2">
            <label className="text-sm text-stone-700">图片 worker 上限</label>
            <Input
              value={String(config.max_image_workers ?? "")}
              onChange={(event) => setMaxImageWorkers(event.target.value)}
              placeholder={displayAuto(config.max_image_workers, imageEffective)}
              className="h-10 rounded-xl border-stone-200 bg-white"
            />
            <p className="text-xs text-stone-500">留空自动计算；手动值会受 32 的硬上限保护。</p>
          </div>
          <div className="space-y-2">
            <label className="text-sm text-stone-700">HTTP 共享线程池</label>
            <Input
              value={String(config.starlette_pool_size ?? "")}
              onChange={(event) => setStarlettePoolSize(event.target.value)}
              placeholder={displayAuto(config.starlette_pool_size, starletteEffective)}
              className="h-10 rounded-xl border-stone-200 bg-white"
            />
            <p className="text-xs text-stone-500">留空使用默认 100；最小有效值为 20。</p>
          </div>
        </div>

        <div className="grid gap-3 md:grid-cols-4">
          <div className="rounded-xl border border-stone-200 bg-stone-50 px-4 py-3">
            <div className="flex items-center gap-2 text-xs text-stone-500">
              <Activity className="size-3.5" />
              图片 worker
            </div>
            <div className="mt-2 text-xl font-semibold text-stone-950">{currentInflight} / {imageEffective || "-"}</div>
          </div>
          <div className="rounded-xl border border-stone-200 bg-stone-50 px-4 py-3">
            <div className="flex items-center gap-2 text-xs text-stone-500">
              <ServerCog className="size-3.5" />
              共享线程池
            </div>
            <div className="mt-2 text-xl font-semibold text-stone-950">{starletteEffective || "-"}</div>
          </div>
          <div className="rounded-xl border border-stone-200 bg-stone-50 px-4 py-3">
            <div className="text-xs text-stone-500">24h 过载拒绝</div>
            <div className="mt-2 text-xl font-semibold text-stone-950">{rejectionCount}</div>
          </div>
          <div className="rounded-xl border border-stone-200 bg-stone-50 px-4 py-3">
            <div className="text-xs text-stone-500">账号并发基数</div>
            <div className="mt-2 text-xl font-semibold text-stone-950">{accountTotal} × {perAccount}</div>
          </div>
        </div>

        {isLoadingInfo ? (
          <div className="flex items-center gap-2 text-xs text-stone-500">
            <LoaderCircle className="size-3.5 animate-spin" />
            正在刷新状态
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
