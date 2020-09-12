worker_num = ENV.fetch("WORKER_NUM", 3)
workers worker_num
app_root = File.expand_path('../', __FILE__)

thread_num = ENV.fetch("PUMA_THREAD_NUM", 16)
threads thread_num, thread_num

port (ENV.fetch('SERVER_PORT', 1323)).to_i
#pidfile "#{app_root}/server.pid"

worker_timeout 60

#daemonize true

preload_app!

stdout_redirect "#{app_root}/puma.log", "#{app_root}/puma_error.log", true


# https://github.com/puma/puma/issues/1957
restart_command "bundle exec --keep-file-descriptors pumactl"

# 以下はあとで直す
before_fork do
  ActiveRecord::Base.connection_pool.disconnect! if defined?(ActiveRecord)
end

on_worker_boot do
  ActiveRecord::Base.establish_connection if defined?(ActiveRecord)
end

on_restart do
  if defined?(REDIS) && REDIS.connected?
    REDIS.close
  end
end
