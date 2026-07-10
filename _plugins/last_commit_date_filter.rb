module Jekyll
  module LastCommitDateFilter
    # Returns "Mon. YYYY" for the last commit touching file_path, or "" if
    # unavailable (missing file, no git history) — caller decides fallback.
    def last_commit_date(file_path)
      require "open3"
      site_source = Jekyll.sites.first&.source || Dir.pwd
      full_path = File.expand_path(file_path, site_source)
      return "" unless File.exist?(full_path)

      output, status = Open3.capture2(
        "git", "-C", site_source, "log", "-1",
        "--format=%cd", "--date=format:%b. %Y", "--", file_path
      )
      status.success? ? output.strip : ""
    end
  end
end

Liquid::Template.register_filter(Jekyll::LastCommitDateFilter)
