#include "aligned_file_reader.h"
#ifdef __APPLE__

#include "apple_aligned_file_reader.h"
#include "utils.h"

#define SECTOR_LEN 4096

AppleAlignedFileReader::AppleAlignedFileReader()
{
    this->file_desc = -1;
    // diskann::cout << "AppleAlignedFileReader created, this=" << this << std::endl;
}

AppleAlignedFileReader::~AppleAlignedFileReader()
{
    // diskann::cout << "AppleAlignedFileReader destructor called, this=" << this << std::endl;

    // Deregister all threads
    deregister_all_threads();

    // Close file descriptor
    if (this->file_desc >= 0)
    {
        // diskann::cout << "Closing file in destructor, fd=" << this->file_desc << std::endl;
        ::close(this->file_desc);
        this->file_desc = -1;
    }
}

IOContext &AppleAlignedFileReader::get_ctx()
{
    auto thread_id = std::this_thread::get_id();

    // Create a static empty context for error cases
    static IOContext empty_ctx;
    static bool initialized = false;

    if (!initialized)
    {
        empty_ctx.queue = nullptr;
        empty_ctx.grp = nullptr;
        empty_ctx.channel = nullptr;
        initialized = true;
    }

    std::unique_lock<std::mutex> lk(this->ctx_mut);

    // If thread is not registered, auto-register it
    if (ctx_map.find(thread_id) == ctx_map.end())
    {
        lk.unlock();
        diskann::cerr << "Thread " << thread_id << " not registered, auto-registering" << std::endl;

        // Auto-register thread
        if (this->file_desc >= 0)
        {
            this->register_thread();

            // Check if registration was successful
            lk.lock();
            if (ctx_map.find(thread_id) != ctx_map.end())
            {
                return ctx_map[thread_id];
            }
            lk.unlock();
        }

        return empty_ctx;
    }

    // If registered, return context
    IOContext &ctx = ctx_map[thread_id];
    lk.unlock();
    return ctx;
}

void AppleAlignedFileReader::register_thread()
{
    auto current_id = std::this_thread::get_id();
    // diskann::cout << "register_thread called from thread " << current_id << " on instance " << this << std::endl;

    // Check if file descriptor is valid
    if (this->file_desc < 0)
    {
        diskann::cerr << "Thread " << current_id << " - register_thread called with invalid file descriptor"
                      << std::endl;
        return;
    }

    // Check if thread is registered
    {
        std::lock_guard<std::mutex> ctx_lock(this->ctx_mut);
        if (ctx_map.find(current_id) != ctx_map.end())
        {
            // diskann::cout << "Thread " << current_id << " already registered" << std::endl;
            return;
        }
    }

    // Create thread context
    IOContext ctx;
    ctx.queue = nullptr;
    ctx.grp = nullptr;
    ctx.channel = nullptr;

    std::string queue_name =
        "diskann_io_" + std::to_string(*static_cast<unsigned int *>(static_cast<void *>(&current_id)));
    ctx.queue = dispatch_queue_create(queue_name.c_str(), DISPATCH_QUEUE_SERIAL);
    if (!ctx.queue)
    {
        diskann::cerr << "Failed to create queue for thread " << current_id << std::endl;
        return;
    }

    ctx.grp = dispatch_group_create();
    if (!ctx.grp)
    {
        diskann::cerr << "Failed to create group for thread " << current_id << std::endl;
        dispatch_release(ctx.queue);
        return;
    }

    // Duplicate file descriptor
    int dup_fd = ::dup(this->file_desc);
    if (dup_fd == -1)
    {
        diskann::cerr << "Failed to duplicate file descriptor: " << this->file_desc << ", errno=" << errno << std::endl;
        dispatch_release(ctx.grp);
        dispatch_release(ctx.queue);
        return;
    }

    // Create IO channel
    ctx.channel = dispatch_io_create(DISPATCH_IO_RANDOM, dup_fd, ctx.queue, ^(int error) {
      ::close(dup_fd);
      // diskann::cout << "IO channel cleanup called, closed fd=" << dup_fd << std::endl;
    });

    if (!ctx.channel)
    {
        diskann::cerr << "Failed to create IO channel for thread " << current_id << ", fd=" << dup_fd
                      << ", errno=" << errno << std::endl;
        ::close(dup_fd);
        dispatch_release(ctx.grp);
        dispatch_release(ctx.queue);
        return;
    }

    // Set IO channel parameters
    dispatch_io_set_low_water(ctx.channel, SECTOR_LEN);
    dispatch_io_set_high_water(ctx.channel, SECTOR_LEN * 16);

    // Add to thread mapping
    {
        std::lock_guard<std::mutex> ctx_lock(this->ctx_mut);
        ctx_map[current_id] = ctx;
    }

    // diskann::cout << "Thread " << current_id << " successfully registered with fd=" << dup_fd << std::endl;
}

void AppleAlignedFileReader::deregister_thread()
{
    auto my_id = std::this_thread::get_id();
    // diskann::cout << "deregister_thread called from thread " << my_id << " on instance " << this << std::endl;

    IOContext ctx;
    bool found = false;

    {
        std::lock_guard<std::mutex> ctx_lock(this->ctx_mut);
        if (ctx_map.find(my_id) != ctx_map.end())
        {
            ctx = ctx_map[my_id];
            ctx_map.erase(my_id);
            found = true;
        }
    }

    if (!found)
    {
        diskann::cerr << "Thread " << my_id << " not registered, cannot deregister" << std::endl;
        return;
    }

    if (ctx.channel)
    {
        dispatch_io_close(ctx.channel, DISPATCH_IO_STOP);
        dispatch_release(ctx.channel);
    }

    if (ctx.grp)
    {
        dispatch_release(ctx.grp);
    }

    if (ctx.queue)
    {
        dispatch_release(ctx.queue);
    }

    // diskann::cout << "Thread " << my_id << " deregistered" << std::endl;
}

void AppleAlignedFileReader::deregister_all_threads()
{
    // diskann::cout << "deregister_all_threads called on instance " << this << std::endl;

    std::vector<IOContext> contexts;

    {
        std::lock_guard<std::mutex> ctx_lock(this->ctx_mut);
        // diskann::cout << "Deregistering " << ctx_map.size() << " threads" << std::endl;
        for (auto &pair : ctx_map)
        {
            contexts.push_back(pair.second);
        }
        ctx_map.clear();
    }

    for (auto &ctx : contexts)
    {
        if (ctx.channel)
        {
            dispatch_io_close(ctx.channel, DISPATCH_IO_STOP);
            dispatch_release(ctx.channel);
        }

        if (ctx.grp)
        {
            dispatch_release(ctx.grp);
        }

        if (ctx.queue)
        {
            dispatch_release(ctx.queue);
        }
    }

    // diskann::cout << "All threads deregistered" << std::endl;
}

void AppleAlignedFileReader::open(const std::string &fname)
{
    // diskann::cout << "open called for file: " << fname << " on instance " << this << std::endl;

    // Close existing file descriptor
    if (this->file_desc >= 0)
    {
        // diskann::cout << "Closing existing file descriptor: " << this->file_desc << std::endl;
        ::close(this->file_desc);
        this->file_desc = -1;
    }

    // Clear all thread contexts
    deregister_all_threads();

    // Open new file
    this->file_desc = ::open(fname.c_str(), O_RDONLY);
    if (this->file_desc == -1)
    {
        diskann::cerr << "💥 DETAILED FILE OPEN FAILURE:" << std::endl;
        diskann::cerr << "   File: " << fname << std::endl;
        diskann::cerr << "   Errno: " << errno << " (" << strerror(errno) << ")" << std::endl;
        diskann::cerr << "   File exists: " << (access(fname.c_str(), F_OK) == 0 ? "YES" : "NO") << std::endl;
        diskann::cerr << "   File readable: " << (access(fname.c_str(), R_OK) == 0 ? "YES" : "NO") << std::endl;
        throw std::runtime_error(std::string("Failed to open file: ") + fname + " (errno=" + std::to_string(errno) +
                                 ")");
    }

    // Get file info
    struct stat file_info;
    if (::fstat(this->file_desc, &file_info) == 0)
    {
        // diskann::cout << "File opened successfully: " << fname << ", size: " << file_info.st_size
        //               << " bytes, fd=" << this->file_desc << std::endl;
    }
    else
    {
        diskann::cout << "File opened but couldn't get file info, fd=" << this->file_desc << std::endl;
    }
}

void AppleAlignedFileReader::close()
{
    // diskann::cout << "close called on instance " << this << std::endl;

    // Clear all thread contexts
    deregister_all_threads();

    // Close file descriptor
    if (this->file_desc >= 0)
    {
        // diskann::cout << "Closing file descriptor: " << this->file_desc << std::endl;
        ::close(this->file_desc);
        this->file_desc = -1;
    }
}

void AppleAlignedFileReader::read(std::vector<AlignedRead> &read_reqs, IOContext &ctx, bool async)
{
    auto thread_id = std::this_thread::get_id();

    // If channel is invalid, auto-register thread
    if (!ctx.channel && this->file_desc >= 0)
    {
        diskann::cout << "Auto-registering thread " << thread_id << " during read" << std::endl;
        this->register_thread();
        // Get new context
        ctx = this->get_ctx();
    }

    // Safety check
    if (!ctx.channel || !ctx.queue || !ctx.grp)
    {
        diskann::cerr << "Invalid IO context in thread " << thread_id << std::endl;
        return;
    }

    dispatch_io_t channel = ctx.channel;
    dispatch_queue_t q = ctx.queue;
    dispatch_group_t group = ctx.grp;

    // Process all read requests
    uint64_t n_reqs = read_reqs.size();
    for (uint64_t i = 0; i < n_reqs; i++)
    {
        AlignedRead &req = read_reqs[i];

        // Check alignment
        if (!IS_ALIGNED(req.buf, SECTOR_LEN) || !IS_ALIGNED(req.offset, SECTOR_LEN) || !IS_ALIGNED(req.len, SECTOR_LEN))
        {
            diskann::cerr << "Thread " << thread_id << " - alignment error for request " << i << std::endl;
            continue;
        }

        dispatch_group_enter(group);

        dispatch_io_read(channel, req.offset, req.len, q, ^(bool done, dispatch_data_t data, int error) {
          if (error)
          {
              diskann::cerr << "Thread " << thread_id << " read error: " << error << " when reading at offset "
                            << req.offset << std::endl;
              if (done)
                  dispatch_group_leave(group);
              return;
          }

          if (data)
          {
              size_t actual_size = dispatch_data_get_size(data);
              if (actual_size > 0)
              {
                  __block size_t total_copied = 0;
                  dispatch_data_apply(data,
                                      ^(dispatch_data_t region, size_t region_offset, const void *buffer, size_t size) {
                                        if (region_offset + size <= req.len)
                                        {
                                            memcpy((char *)req.buf + region_offset, buffer, size);
                                            total_copied += size;
                                            return (bool)true;
                                        }
                                        diskann::cerr << "Buffer overflow: region_offset=" << region_offset
                                                      << ", size=" << size << ", req.len=" << req.len << std::endl;
                                        return (bool)false;
                                      });

                  if (total_copied != req.len && done)
                  {
                      diskann::cerr << "Warning: Only copied " << total_copied << " of " << req.len
                                    << " requested bytes" << std::endl;
                  }
              }
          }

          // Only leave group when done
          if (done)
          {
              dispatch_group_leave(group);
          }
        });
    }

    dispatch_group_wait(group, DISPATCH_TIME_FOREVER);
}

#endif