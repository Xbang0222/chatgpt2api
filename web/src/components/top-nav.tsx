"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { Github } from "lucide-react";
import { usePathname, useRouter } from "next/navigation";

import webConfig from "@/constants/common-env";
import { ThemeToggle } from "@/components/theme-toggle";
import { getValidatedAuthSession } from "@/lib/auth-session";
import { cn } from "@/lib/utils";
import { clearStoredAuthSession, type StoredAuthSession } from "@/store/auth";

const adminNavItems = [
  { href: "/image", label: "画图" },
  { href: "/accounts", label: "号池管理" },
  { href: "/register", label: "注册机" },
  { href: "/image-manager", label: "图片管理" },
  { href: "/logs", label: "日志管理" },
  { href: "/settings", label: "设置" },
];

const userNavItems = [{ href: "/image", label: "画图" }];

export function TopNav() {
  const pathname = usePathname();
  const router = useRouter();
  const [session, setSession] = useState<StoredAuthSession | null | undefined>(undefined);

  useEffect(() => {
    let active = true;

    const load = async () => {
      if (pathname === "/login") {
        if (!active) {
          return;
        }
        setSession(null);
        return;
      }

      const storedSession = await getValidatedAuthSession();
      if (!active) {
        return;
      }
      setSession(storedSession);
    };

    void load();
    return () => {
      active = false;
    };
  }, [pathname]);

  const handleLogout = async () => {
    await clearStoredAuthSession();
    router.replace("/login");
  };

  if (pathname === "/login" || session === undefined || !session) {
    return null;
  }

  const navItems = session.role === "admin" ? adminNavItems : userNavItems;
  const roleLabel = session.role === "admin" ? "管理员" : "普通用户";
  const displayName = session.name.trim() || roleLabel;

  return (
    <header className="border-b border-stone-100/50 dark:border-white/10">
      <div className="flex min-h-12 flex-col gap-1 px-3 py-2 sm:h-12 sm:flex-row sm:items-center sm:justify-between sm:gap-3 sm:px-6 sm:py-0">
        <div className="flex items-center justify-between gap-2 sm:justify-start sm:gap-3">
          <Link
            href="/image"
            className="shrink-0 py-1 text-[15px] font-bold tracking-tight text-stone-950 transition hover:text-stone-700 dark:text-stone-50 dark:hover:text-white"
          >
            chatgpt2api
          </Link>
          <a
            href="https://github.com/basketikun/chatgpt2api"
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1.5 py-1 text-sm text-stone-400 transition hover:text-stone-700 dark:text-stone-500 dark:hover:text-stone-200"
            aria-label="GitHub repository"
          >
            <Github className="size-4" />
            <span className="hidden md:inline">GitHub</span>
          </a>
          <div className="ml-auto sm:hidden">
            <ThemeToggle />
          </div>
          <button
            type="button"
            className="shrink-0 py-1 text-xs text-stone-400 transition hover:text-stone-700 dark:text-stone-500 dark:hover:text-stone-200 sm:hidden"
            onClick={() => void handleLogout()}
          >
            退出
          </button>
        </div>
        <nav className="hide-scrollbar -mx-1 flex min-w-0 flex-1 gap-1 overflow-x-auto px-1 sm:mx-0 sm:justify-center sm:gap-8 sm:overflow-visible sm:px-0">
          {navItems.map((item) => {
            const active = pathname === item.href;
            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  "relative shrink-0 whitespace-nowrap rounded-full px-2.5 py-1 text-[13px] font-medium transition sm:rounded-none sm:px-0 sm:text-[15px]",
                  active
                    ? "bg-stone-950 text-white sm:bg-transparent sm:font-semibold sm:text-stone-950 dark:bg-white dark:text-stone-950 dark:sm:bg-transparent dark:sm:text-white"
                    : "text-stone-500 hover:text-stone-900 dark:text-stone-400 dark:hover:text-stone-100",
                )}
              >
                {item.label}
                {active ? <span className="absolute inset-x-0 -bottom-[1px] hidden h-0.5 bg-stone-950 dark:bg-white sm:block" /> : null}
              </Link>
            );
          })}
        </nav>
        <div className="hidden items-center justify-end gap-2 sm:flex sm:gap-3">
          <ThemeToggle />
          <span className="hidden rounded-md bg-stone-100 px-2 py-1 text-[10px] font-medium text-stone-500 dark:bg-white/8 dark:text-stone-300 sm:inline-block sm:text-[11px]">
            {roleLabel} · {displayName}
          </span>
          <span className="hidden rounded-md bg-stone-100 px-2 py-1 text-[10px] font-medium text-stone-500 dark:bg-white/8 dark:text-stone-300 sm:inline-block sm:text-[11px]">
            v{webConfig.appVersion}
          </span>
          <button
            type="button"
            className="py-1 text-xs text-stone-400 transition hover:text-stone-700 dark:text-stone-500 dark:hover:text-stone-200 sm:text-sm"
            onClick={() => void handleLogout()}
          >
            退出
          </button>
        </div>
      </div>
    </header>
  );
}
