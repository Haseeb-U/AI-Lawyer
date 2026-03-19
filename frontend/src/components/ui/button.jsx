import * as React from "react"
import { Slot } from "@radix-ui/react-slot"
import { cva } from "class-variance-authority"

import { cn } from "@/lib/utils"

const buttonVariants = cva(
  "inline-flex items-center justify-center whitespace-nowrap rounded-full text-sm font-medium transition-all duration-300 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-brand-500 disabled:pointer-events-none disabled:opacity-50",
  {
    variants: {
      variant: {
        default:
          "bg-[var(--brand-500)] text-slate-900 font-bold shadow hover:bg-[var(--brand-600)] hover:shadow-[0_0_20px_rgba(212,175,55,0.4)] border border-transparent",
        destructive:
          "bg-red-500 text-neutral-50 hover:bg-red-500/90",
        outline:
          "border border-[var(--card-border)] bg-transparent hover:bg-black/5 dark:hover:bg-white/10 text-[var(--foreground)]",
        secondary:
          "bg-[var(--card-border)] text-[var(--foreground)] hover:brightness-95 dark:hover:brightness-110",
        ghost: "hover:bg-[var(--card-border)] hover:text-[var(--foreground)]",
        link: "text-brand-500 underline-offset-4 hover:underline",
      },
      size: {
        default: "h-9 px-4 py-2",
        sm: "h-8 rounded-full px-3 text-xs",
        lg: "h-11 rounded-full px-8 py-3.5",
        icon: "h-9 w-9",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  }
)

const Button = React.forwardRef(({ className, variant, size, asChild = false, ...props }, ref) => {
  const Comp = asChild ? Slot : "button"
  return (
    (<Comp
      className={cn(buttonVariants({ variant, size, className }))}
      ref={ref}
      {...props} />)
  );
})
Button.displayName = "Button"

// eslint-disable-next-line react-refresh/only-export-components
export { Button, buttonVariants }