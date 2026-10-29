using Microsoft.AspNetCore.Mvc;
using System;
using Coflnet.Sky.SkyAuctionTracker.Services;
using Microsoft.Extensions.DependencyInjection;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace Coflnet.Sky.SkyAuctionTracker.Controllers
{
    [ApiController]
    public class MigrationController : ControllerBase
    {
        IServiceProvider provider;

        public MigrationController(IServiceProvider provider)
        {
            this.provider = provider;
        }

        [Route("ready")]
        [HttpGet]
        public IActionResult Ready()
        {
            var migrationService = provider.GetService<BazaarService>();
            if(migrationService.LastSuccessfullDB > DateTime.UtcNow - TimeSpan.FromMinutes(5))
            {
                return Ok("success");
            }
            return StatusCode(503);
        }

        /// <summary>Read-only CQL connection diagnostic; existing freshness readiness remains /ready.</summary>
        [HttpGet("health/ready")]
        public async Task<IActionResult> ConnectionReady(CancellationToken cancellationToken)
        {
            try
            {
                var statement = new SimpleStatement("SELECT key FROM system.local").SetReadTimeoutMillis(5000);
                await provider.GetRequiredService<ISession>().ExecuteAsync(statement)
                    .WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
                return Ok("ready");
            }
            catch (Exception)
            {
                return StatusCode(503, "not ready");
            }
        }
    }
}
